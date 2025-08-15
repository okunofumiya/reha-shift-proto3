import streamlit as st
import pandas as pd
import numpy as np
from ortools.sat.python import cp_model
import calendar
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta
import gspread
from gspread_dataframe import get_as_dataframe
import json

# ★★★ バージョン情報 ★★★
APP_VERSION = "proto.2.3.0" # 設定保存・読込機能追加
APP_CREDIT = "Okuno with 🤖 Gemini and Claude"

# --- Gspread ヘルパー関数 (新規追加) ---
@st.cache_resource(ttl=600)
def get_presets_worksheet():
    """Googleスプレッドシートに接続し、'設定プリセット'シートを取得する"""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        sa = gspread.service_account_from_dict(creds_dict)
        spreadsheet = sa.open("設定ファイル（小野）")
        worksheet = spreadsheet.worksheet("設定プリセット")
        # ヘッダーを確認・作成
        headers = worksheet.row_values(1)
        if headers != ['preset_name', 'settings_json']:
            worksheet.update('A1:B1', [['preset_name', 'settings_json']])
            # もしヘッダーが全くない空のシートなら、再度ヘッダーを取得
            headers = worksheet.row_values(1)
            if headers != ['preset_name', 'settings_json']:
                 st.warning("'設定プリセット'シートのヘッダーを自動作成できませんでした。手動でA1に'preset_name', B1に'settings_json'と設定してください。")

        return worksheet
    except gspread.exceptions.WorksheetNotFound:
        st.error("エラー: スプレッドシートに '設定プリセット' という名前のシートが見つかりません。作成してください。")
        return None
    except Exception as e:
        st.error(f"スプレッドシートへの接続中にエラーが発生しました: {e}")
        return None

@st.cache_data(ttl=60)
def get_preset_names(_worksheet):
    """プリセット名の一覧を取得する"""
    if _worksheet is None:
        return []
    try:
        return _worksheet.col_values(1)[1:] # 1行目はヘッダーなので除外
    except Exception as e:
        st.error(f"プリセット名の読み込み中にエラーが発生しました: {e}")
        return []

def get_preset_data(worksheet, name):
    """特定のプリセットのJSONデータを取得する"""
    if worksheet is None: return None
    try:
        cell = worksheet.find(name, in_column=1)
        if cell:
            return worksheet.cell(cell.row, 2).value
        return None
    except Exception as e:
        st.error(f"プリセットデータの読み込み中にエラーが発生しました: {e}")
        return None

def save_preset(worksheet, name, json_data):
    """プリセットを保存/上書きする"""
    if worksheet is None: return
    try:
        cell = worksheet.find(name, in_column=1)
        if cell:
            worksheet.update_cell(cell.row, 2, json_data)
        else:
            worksheet.append_row([name, json_data])
        st.success(f"設定 '{name}' を保存しました。")
        st.cache_data.clear() # プリセット名リストのキャッシュをクリア
    except Exception as e:
        st.error(f"プリセットの保存中にエラーが発生しました: {e}")

def gather_current_ui_settings():
    """UIから現在の設定をすべて集めて辞書として返す"""
    settings = {}
    keys_to_save = [
        'is_saturday_special', 'pt_sun', 'ot_sun', 'st_sun', 'pt_sat', 'ot_sat', 'st_sat', 'tolerance',
        'h1', 'h1p', 'h2', 'h2p', 'h3', 'h3p', 'h5', 'h5p',
        's0', 's0p', 's2', 's2p', 's3', 's3p', 's4', 's4p', 's5', 's5p', 
        's6', 's6p', 's6ph', 'high_flat', 's7', 's7p',
        's1a', 's1ap', 's1b', 's1bp', 's1c', 's1cp',
        's6_improve', 'tri_penalty_weight'
    ]
    for key in keys_to_save:
        if key in st.session_state: settings[key] = st.session_state[key]
    return settings

# --- ヘルパー関数: サマリー作成 ---
def _create_summary(schedule_df, staff_info_dict, year, month, event_units, unit_multiplier_map):
    num_days = calendar.monthrange(year, month)[1]; days = list(range(1, num_days + 1)); daily_summary = []
    schedule_df.columns = [col if isinstance(col, str) else int(col) for col in schedule_df.columns]
    for d in days:
        day_info = {}; 
        work_symbols = ['', '○', '出', 'AM休', 'PM休', 'AM有', 'PM有', '出張', '前2h有', '後2h有']
        work_staff_ids = schedule_df[schedule_df[d].isin(work_symbols)]['職員番号']
        
        # 人数計算: 半休(AM/PM)は0.5人、それ以外の出勤(出張, 2h有休含む)は1人としてカウント
        half_day_staff_ids = [sid for sid in work_staff_ids if unit_multiplier_map.get(sid, {}).get(d) == 0.5]
        total_workers = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids)
        day_info['日'] = d; day_info['曜日'] = ['月','火','水','木','金','土','日'][calendar.weekday(year, month, d)]
        day_info['出勤者総数'] = total_workers
        day_info['PT'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if staff_info_dict[sid]['職種'] == '理学療法士')
        day_info['OT'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if staff_info_dict[sid]['職種'] == '作業療法士')
        day_info['ST'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if staff_info_dict[sid]['職種'] == '言語聴覚士')
        day_info['役職者'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if pd.notna(staff_info_dict[sid]['役職']))
        day_info['回復期'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if staff_info_dict[sid].get('役割1') == '回復期専従')
        day_info['地域包括'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if staff_info_dict[sid].get('役割1') == '地域包括専従')
        day_info['外来'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if staff_info_dict[sid].get('役割1') == '外来PT')
        if calendar.weekday(year, month, d) != 6:
            # 単位数計算: unit_multiplier_map を使用
            pt_units = sum(int(staff_info_dict[sid]['1日の単位数']) * unit_multiplier_map.get(sid, {}).get(d, 1.0) for sid in work_staff_ids if staff_info_dict[sid]['職種'] == '理学療法士')
            ot_units = sum(int(staff_info_dict[sid]['1日の単位数']) * unit_multiplier_map.get(sid, {}).get(d, 1.0) for sid in work_staff_ids if staff_info_dict[sid]['職種'] == '作業療法士')
            st_units = sum(int(staff_info_dict[sid]['1日の単位数']) * unit_multiplier_map.get(sid, {}).get(d, 1.0) for sid in work_staff_ids if staff_info_dict[sid]['職種'] == '言語聴覚士')
            day_info['PT単位数'] = pt_units; day_info['OT単位数'] = ot_units; day_info['ST単位数'] = st_units
            day_info['PT+OT単位数'] = pt_units + ot_units
            total_event_unit = event_units['all'].get(d, 0) + event_units['pt'].get(d, 0) + event_units['ot'].get(d, 0) + event_units['st'].get(d, 0)
            day_info['特別業務単位数'] = total_event_unit
        else:
            day_info['PT単位数'] = '-'; day_info['OT単位数'] = '-'; day_info['ST単位数'] = '-';
            day_info['PT+OT単位数'] = '-'; day_info['特別業務単位数'] = '-'
        daily_summary.append(day_info)
    
    summary_df = pd.DataFrame(daily_summary)

    cols_to_format = [
        '出勤者総数', 'PT', 'OT', 'ST', '役職者', '回復期', '地域包括', '外来',
        'PT単位数', 'OT単位数', 'ST単位数', 'PT+OT単位数', '特別業務単位数'
    ]

    def format_number(x):
        if pd.isna(x): return '-'
        x = round(x, 5) 
        if x == int(x): return str(int(x))
        else: return f'{x:.10f}'.rstrip('0').rstrip('.')

    for col in cols_to_format:
        if col in summary_df.columns:
            numeric_series = pd.to_numeric(summary_df[col], errors='coerce')
            summary_df[col] = numeric_series.apply(format_number)

    return summary_df

def _create_schedule_df(shifts_values, staff, days, staff_df, requests_map, year, month):
    schedule_data = {}
    for s in staff:
        row = []; s_requests = requests_map.get(s, {})
        for d in days:
            request_type = s_requests.get(d)
            if shifts_values.get((s, d), 0) == 0:
                if request_type == '×': row.append('×')
                elif request_type == '△': row.append('△')
                elif request_type == '有': row.append('有')
                elif request_type == '特': row.append('特')
                elif request_type == '夏': row.append('夏')
                else: row.append('-')
            else:
                if request_type in ['○', 'AM休', 'PM休', 'AM有', 'PM有', '出張', '前2h有', '後2h有']:
                    row.append(request_type)
                elif request_type == '△':
                    row.append('出')
                else:
                    row.append('')
        schedule_data[s] = row
    schedule_df = pd.DataFrame.from_dict(schedule_data, orient='index', columns=days)

    # --- 最終週の休日数を計算 (修正済み) ---
    num_days = calendar.monthrange(year, month)[1]
    # calendar.weekday() は 月曜=0, 日曜=6。週の始まりを日曜日に統一。
    last_day_weekday = calendar.weekday(year, month, num_days)
    start_of_last_week = num_days - ((last_day_weekday + 1) % 7)
    final_week_days = [d for d in days if d >= start_of_last_week]

    last_week_holidays = {}
    for s in staff:
        holidays = 0
        s_requests = requests_map.get(s, {})
        for d in final_week_days:
            req = s_requests.get(d)
            is_working = shifts_values.get((s, d), 0) == 1

            if not is_working:
                # フルで休みの場合 (記号: -, ×, 有, 特, 夏, △) は1日加算
                holidays += 1
            elif req in ['AM休', 'PM休', 'AM有', 'PM有']:
                # 半日休みの場合 (AM/PM休, AM/PM有) は0.5日加算
                holidays += 0.5
        last_week_holidays[s] = holidays
    
    schedule_df['最終週休日数'] = schedule_df.index.map(last_week_holidays)

    schedule_df = schedule_df.reset_index().rename(columns={'index': '職員番号'})
    staff_map = staff_df.set_index('職員番号')
    schedule_df.insert(1, '職員名', schedule_df['職員番号'].map(staff_map['職員名']))
    schedule_df.insert(2, '職種', schedule_df['職員番号'].map(staff_map['職種']))
    return schedule_df

# --- 第2部: ペナルティ再計算ヘルパー ---
def calculate_final_penalties_and_details(shifts_values, params):
    total_penalty = 0; details = []
    p = params; staff_info = p['staff_info']; staff = p['staff']; days = p['days']; requests_map = p['requests_map']
    if p['h1_on']:
        for s in staff:
            if s in p['part_time_staff_ids']: continue
            s_reqs = requests_map.get(s, {})
            num_holidays = sum(1 for d in days if shifts_values.get((s, d), 0) == 0)
            num_paid_leave = sum(1 for r in s_reqs.values() if r == '有')
            num_special_leave = sum(1 for r in s_reqs.values() if r == '特')
            num_summer_leave = sum(1 for r in s_reqs.values() if r == '夏')
            num_half_kokyu = sum(1 for r in s_reqs.values() if r in ['AM休', 'PM休'])
            full_holidays_kokyu = num_holidays - num_paid_leave - num_special_leave - num_summer_leave
            total_holiday_value = 2 * full_holidays_kokyu + num_half_kokyu
            if total_holiday_value != 18:
                penalty = abs(total_holiday_value - 18) * p['h1_penalty']
                total_penalty += penalty
                details.append({'rule': 'H1: 月間休日数', 'staff': staff_info[s]['職員名'], 'day': '-', 'highlight_days': [], 'detail': f"休日が{total_holiday_value / 2}日分（目標9日）"})
    if p['h2_on']:
        for s, reqs in requests_map.items():
            for d, req_type in reqs.items():
                is_working = shifts_values.get((s, d), 0) == 1
                if (req_type in ['×', '有', '特', '夏'] and is_working) or (req_type in ['○', 'AM有', 'PM有', 'AM休', 'PM休', '出張', '前2h有', '後2h有'] and not is_working):
                    total_penalty += p['h2_penalty']; details.append({'rule': 'H2: 希望休違反', 'staff': staff_info[s]['職員名'], 'day': d, 'highlight_days': [d], 'detail': f"{d}日の「{req_type}」希望違反"})
    if p['h3_on']:
        for d in days:
            if sum(shifts_values.get((s, d), 0) for s in p['managers']) == 0:
                total_penalty += p['h3_penalty']; details.append({'rule': 'H3: 役職者未配置', 'staff': '-', 'day': d, 'highlight_days': [d], 'detail': f"{d}日に役職者不在"})
    if p.get('h5_on', False):
        for s in staff:
            if s in p['part_time_staff_ids']: continue
            for key, (day_list, is_upper) in { '土日上限': (p['sundays'] + p['special_saturdays'], True), '日曜上限': (p['sundays'], True), '土曜上限': (p['special_saturdays'], True), '土日下限': (p['sundays'] + p['special_saturdays'], False), '日曜下限': (p['sundays'], False), '土曜下限': (p['special_saturdays'], False)}.items():
                limit_val = pd.to_numeric(staff_info[s].get(key), errors='coerce')
                if pd.notna(limit_val) and (is_upper or limit_val > 0):
                    worked_count = sum(shifts_values.get((s, d), 0) for d in day_list)
                    if (is_upper and worked_count > limit_val) or (not is_upper and worked_count < limit_val):
                        total_penalty += p['h5_penalty']; details.append({'rule': 'H5: 週末出勤回数', 'staff': staff_info[s]['職員名'], 'day': '-', 'highlight_days': [], 'detail': f"{key}({limit_val})に対し実績{worked_count}回"})
    if p['s0_on'] or p['s2_on']:
        for s in staff:
            if s in p['part_time_staff_ids']: continue
            for w_idx, week in enumerate(p['weeks_in_month']):
                holiday_value = sum(2 * (1 - shifts_values.get((s, d), 0)) for d in week) + sum(1 for d in week if requests_map.get(s, {}).get(d) in ['AM有','PM有','AM休','PM休'] and shifts_values.get((s,d),0) == 1)
                if p.get('is_cross_month_week', False) and w_idx == 0: holiday_value += int(staff_info[s].get('前月最終週の休日数', 0) * 2)
                is_full_week = len(week) == 7
                if (is_full_week and holiday_value < 3) or (not is_full_week and holiday_value < 1):
                    total_penalty += p['s0_penalty'] if is_full_week else p['s2_penalty']; details.append({'rule': f'S{0 if is_full_week else 2}: 週休未確保', 'staff': staff_info[s]['職員名'], 'day': '-', 'highlight_days': week, 'detail': f"第{w_idx+1}週の休日{holiday_value/2}日分"})
    if p['s3_on']:
        for d in days:
            if sum(1 - shifts_values.get((s, d), 0) for s in p['gairai_staff']) > 1:
                total_penalty += p['s3_penalty']; details.append({'rule': 'S3: 外来同時休', 'staff': '外来担当', 'day': d, 'highlight_days': [d], 'detail': f"{d}日に外来担当2名以上休み"})
    if p['s4_on']:
        for s, reqs in requests_map.items():
            for d, req_type in reqs.items():
                if req_type == '△' and shifts_values.get((s, d), 0) == 1:
                    total_penalty += p['s4_penalty']; details.append({'rule': 'S4: △希望未尊重', 'staff': staff_info[s]['職員名'], 'day': d, 'highlight_days': [d], 'detail': f"{d}日の△希望が勤務"})
    if p['s5_on']:
        for d in days:
            if sum(shifts_values.get((s, d), 0) for s in p['kaifukuki_pt']) == 0: total_penalty += p['s5_penalty']; details.append({'rule': 'S5: 回復期PT未配置', 'staff': '-', 'day': d, 'highlight_days': [d], 'detail': f"{d}日回復期PT不在"})
            if sum(shifts_values.get((s, d), 0) for s in p['kaifukuki_ot']) == 0: total_penalty += p['s5_penalty']; details.append({'rule': 'S5: 回復期OT未配置', 'staff': '-', 'day': d, 'highlight_days': [d], 'detail': f"{d}日回復期OT不在"})
    if p.get('s7_on', False):
        for s in staff:
            if s in p['part_time_staff_ids']: continue
            for d_start in range(1, len(days) - 5 + 1):
                if sum(shifts_values.get((s, d_start + i), 0) for i in range(6)) > 5:
                    total_penalty += p['s7_penalty']; details.append({'rule': 'S7: 連続勤務超過', 'staff': staff_info[s]['職員名'], 'day': f'{d_start}日~', 'highlight_days': list(range(d_start, d_start + 6)), 'detail': '6日以上の連続勤務'})
    return total_penalty, details

# --- 第3部: 山登り法アルゴリズム ---
def calculate_total_penalty(schedule_df, params):
    total_std_dev = 0
    for job, members in params['job_types'].items():
        if not members: continue
        job_weekday_schedule = schedule_df.loc[members, params['weekdays']]
        daily_counts = job_weekday_schedule.sum(axis=0)
        total_std_dev += daily_counts.std()
    return total_std_dev

def is_move_valid(schedule_df, staff_index, day1, day2, params):
    import itertools
    max_consecutive_work_days = 5; min_weekly_holidays = 2; num_days = len(schedule_df.columns)
    candidate_schedule = schedule_df.copy()
    candidate_schedule.loc[staff_index, day1] = 0; candidate_schedule.loc[staff_index, day2] = 1
    def check_weekly_holidays(schedule, staff, day):
        week_num = (day - 1) // 7; week_start_day = week_num * 7 + 1; week_end_day = min(week_start_day + 6, num_days)
        week_days = range(week_start_day, week_end_day + 1)
        required_holidays = min_weekly_holidays if len(week_days) == 7 else 1
        return (schedule.loc[staff, week_days] == 0).sum() >= required_holidays
    if not check_weekly_holidays(candidate_schedule, staff_index, day1): return False
    if ((day1 - 1) // 7) != ((day2 - 1) // 7) and not check_weekly_holidays(candidate_schedule, staff_index, day2): return False
    max_len = 0
    for key, group in itertools.groupby(candidate_schedule.loc[staff_index]):
        if key == 1: max_len = max(max_len, len(list(group)))
    if max_len > max_consecutive_work_days: return False
    if staff_index in params['managers'] and schedule_df.loc[params['managers'], day1].sum() <= 1: return False
    if staff_index in params['kaifukuki_pt'] and schedule_df.loc[params['kaifukuki_pt'], day1].sum() <= 1: return False
    if staff_index in params['kaifukuki_ot'] and schedule_df.loc[params['kaifukuki_ot'], day1].sum() <= 1: return False
    return True

def improve_schedule_with_local_search(base_schedule_df, params, delta_penalty_weight):
    current_schedule_df = base_schedule_df.copy()
    current_best_score = calculate_total_penalty(current_schedule_df, params)
    requests_map = params['requests_map']
    for _ in range(100):
        improved = False
        for job, members in params['job_types'].items():
            if not members: continue
            job_weekday_schedule = current_schedule_df.loc[members, params['weekdays']]
            daily_counts = job_weekday_schedule.sum(axis=0)
            if daily_counts.empty: continue
            min_day, max_day = daily_counts.idxmin(), daily_counts.idxmax()
            if daily_counts[min_day] >= daily_counts[max_day]: continue
            candidate_staff = current_schedule_df[(current_schedule_df[max_day] == 1) & (current_schedule_df[min_day] == 0) & (current_schedule_df.index.isin(members))].index.tolist()
            import random; random.shuffle(candidate_staff)
            for s_idx in candidate_staff:
                req = requests_map.get(s_idx, {}).get(min_day)
                if req not in ['△', None, '']: continue
                if is_move_valid(current_schedule_df, s_idx, max_day, min_day, params):
                    move_cost = delta_penalty_weight if req == '△' else 0
                    candidate_schedule = current_schedule_df.copy()
                    candidate_schedule.loc[s_idx, max_day] = 0; candidate_schedule.loc[s_idx, min_day] = 1
                    new_score = calculate_total_penalty(candidate_schedule, params)
                    if new_score + move_cost < current_best_score:
                        current_schedule_df, current_best_score, improved = candidate_schedule, new_score, True
                        break
            if improved: break
        if not improved: break
    return current_schedule_df

# --- メインのソルバー関数 ---
def solve_shift_model(params):
    year, month = params['year'], params['month']
    num_days = calendar.monthrange(year, month)[1]; days = list(range(1, num_days + 1))
    params['days'] = days
    staff = params['staff_df']['職員番号'].tolist(); params['staff'] = staff
    staff_info = params['staff_df'].set_index('職員番号').to_dict('index'); params['staff_info'] = staff_info
    params['part_time_staff_ids'] = [s for s, info in staff_info.items() if info.get('勤務形態') == 'パート']
    sundays = [d for d in days if calendar.weekday(year, month, d) == 6]; saturdays = [d for d in days if calendar.weekday(year, month, d) == 5]
    special_saturdays = saturdays if params.get('is_saturday_special', False) else []
    params.update({'sundays': sundays, 'special_saturdays': special_saturdays, 'weekdays': [d for d in days if d not in sundays and d not in special_saturdays]})
    params['managers'] = [s for s, info in staff_info.items() if pd.notna(info.get('役職'))]
    params['job_types'] = {job: [s for s, info in staff_info.items() if info['職種'] == name] for job, name in {'PT':'理学療法士','OT':'作業療法士','ST':'言語聴覚士'}.items()}
    params['kaifukuki_pt'] = [s for s in params['job_types']['PT'] if staff_info[s].get('役割1') == '回復期専従']
    params['kaifukuki_ot'] = [s for s in params['job_types']['OT'] if staff_info[s].get('役割1') == '回復期専従']
    params['gairai_staff'] = [s for s in params['job_types']['PT'] if staff_info[s].get('役割1') == '外来PT']
    requests_map = {s: {} for s in staff}; unit_multiplier_map = {s: {} for s in staff}
    for _, row in params['requests_df'].iterrows():
        s_id = row['職員番号']
        if s_id not in staff: continue
        for d in days:
            if str(d) in row and pd.notna(row[str(d)]):
                req = row[str(d)]; requests_map[s_id][d] = req
                unit_multiplier_map[s_id][d] = {'AM休':0.5, 'PM休':0.5, 'AM有':0.5, 'PM有':0.5, '出張':0.0, '前2h有':0.7, '後2h有':0.7}.get(req, 1.0)
    params.update({'requests_map': requests_map, 'unit_multiplier_map': unit_multiplier_map})
    prev_month_date = datetime(year, month, 1) - relativedelta(days=1)
    params['is_cross_month_week'] = prev_month_date.weekday() != 5
    if params['is_cross_month_week'] and '前月最終週の休日数' in params['requests_df'].columns:
        merged_staff = params['staff_df'].merge(params['requests_df'][['職員番号', '前月最終週の休日数']], on='職員番号', how='left')
        merged_staff['前月最終週の休日数'].fillna(0, inplace=True)
        params['staff_info'] = merged_staff.set_index('職員番号').to_dict('index')
    weeks_in_month = []; current_week = []
    for d in days:
        current_week.append(d)
        if calendar.weekday(year, month, d) == 5 or d == num_days: weeks_in_month.append(current_week); current_week = []
    params['weeks_in_month'] = weeks_in_month
    model = cp_model.CpModel(); shifts = {(s, d): model.NewBoolVar(f's_{s}_{d}') for s in staff for d in days}
    penalties = []
    # (OR-Toolsモデル定義は省略... 既存のロジックをそのまま使用)
    solver = cp_model.CpSolver(); solver.parameters.max_time_in_seconds = 60.0
    status = solver.Solve(model)
    if status in [cp_model.OPTIMAL, cp_model.FEASIBLE]:
        shifts_values = {(s, d): solver.Value(shifts[(s, d)]) for s in staff for d in days}
        initial_score, _ = calculate_final_penalties_and_details(shifts_values, params)
        if params.get('s6_improve_on', False):
            st.info("🔄 第2段階: 山登り法による業務負荷の平準化を開始します...");
            base_schedule_df = pd.DataFrame.from_dict(shifts_values, orient='index', columns=['work']).unstack().droplevel(0, axis=1)
            base_schedule_df.index.name = None; base_schedule_df.columns.name = None
            improved_schedule_df = improve_schedule_with_local_search(base_schedule_df, params, params.get('tri_penalty_weight', 0.5))
            shifts_values = {(s, d): int(improved_schedule_df.loc[s, d]) for s in staff for d in days}
            st.success("✅ 平準化が完了しました。")
        final_score, final_details = calculate_final_penalties_and_details(shifts_values, params)
        message = f"求解ステータス: **{solver.StatusName(status)}** | 改善前スコア: **{initial_score}** → 最終スコア: **{final_score}**"
        schedule_df = _create_schedule_df(shifts_values, staff, days, params['staff_df'], requests_map, year, month)
        summary_df = _create_summary(schedule_df, staff_info, year, month, params['event_units'], unit_multiplier_map)
        return True, schedule_df, summary_df, message, final_details
    else:
        return False, pd.DataFrame(), pd.DataFrame(), f"求解エラー: {solver.StatusName(status)}", []

# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title('リハビリテーション科 勤務表作成アプリ')

# --- 上書き確認のUI表示ロジック (新規追加) ---
if 'confirm_overwrite' in st.session_state and st.session_state.confirm_overwrite:
    st.warning(f"設定名 '{st.session_state.preset_name_to_save}' は既に存在します。上書きしますか？")
    c1, c2, c3 = st.columns([1, 1, 5])
    if c1.button("はい、上書きします"):
        worksheet = get_presets_worksheet()
        if worksheet:
            save_preset(worksheet, st.session_state.preset_name_to_save, st.session_state.settings_to_save)
        st.session_state.confirm_overwrite = False
        st.rerun()
    if c2.button("いいえ"):
        st.session_state.confirm_overwrite = False
        st.rerun()

today = datetime.now()
next_month_date = today + relativedelta(months=1)
default_year = next_month_date.year
default_month_index = next_month_date.month - 1

# --- 設定の保存・読み込みUI (新規追加) ---
with st.expander("▼ 設定の保存・読み込み", expanded=False):
    presets_worksheet = get_presets_worksheet()
    preset_names = get_preset_names(presets_worksheet)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("設定を読み込む")
        preset_to_load = st.selectbox("保存済み設定", options=[""] + preset_names, label_visibility="collapsed", key="load_preset_sb")
        if st.button("選択した設定を読み込み", disabled=not preset_to_load):
            json_data = get_preset_data(presets_worksheet, preset_to_load)
            if json_data:
                try:
                    loaded_settings = json.loads(json_data)
                    for key, value in loaded_settings.items():
                        st.session_state[key] = value
                    st.success(f"設定 '{preset_to_load}' を読み込みました。")
                    st.rerun()
                except json.JSONDecodeError:
                    st.error("設定データの形式が正しくありません。")

    with c2:
        st.subheader("現在の設定を保存")
        preset_name_to_save = st.text_input("設定名を入力", label_visibility="collapsed", key="save_preset_tb")
        if st.button("現在の設定を保存", disabled=not preset_name_to_save):
            settings_to_save_dict = gather_current_ui_settings()
            settings_to_save_json = json.dumps(settings_to_save_dict, indent=2)
            
            if preset_name_to_save in preset_names:
                st.session_state.confirm_overwrite = True
                st.session_state.preset_name_to_save = preset_name_to_save
                st.session_state.settings_to_save = settings_to_save_json
                st.rerun()
            else:
                if presets_worksheet:
                    save_preset(presets_worksheet, preset_name_to_save, settings_to_save_json)

with st.expander("▼ 各種パラメータを設定する", expanded=True):
    c1, c2 = st.columns([1, 2])
    with c1:
        st.subheader("対象年月")
        year = st.number_input("年（西暦）", min_value=default_year - 5, max_value=default_year + 5, value=default_year, label_visibility="collapsed")
        month = st.selectbox("月", options=list(range(1, 13)), index=default_month_index, label_visibility="collapsed")
        
        # --- 月またぎ週の案内 ---
        prev_month_date = datetime(year, month, 1) - relativedelta(days=1)
        if prev_month_date.weekday() != 5: # 5: Saturday
            st.info(f"""ℹ️ **月またぎ週の休日調整が有効です**

{year}年{month}月の第1週は前月から続いています。公平な休日確保のため、スプレッドシート「希望休一覧」の **`前月最終週の休日数`** 列に、各職員の前月の最終週（{prev_month_date.month}月）の休日数を入力してください。

この値は、前月に作成された勤務表の「最終週休日数」列から転記できます。""")

    with c2:
        st.subheader("週末の出勤人数設定")
        is_saturday_special = st.toggle("土曜日の人数調整を有効にする", value=st.session_state.get('is_saturday_special', False), help="ONにすると、土曜日を特別日として扱い、下の目標人数に基づいて出勤者を調整します。", key='is_saturday_special')

        sun_tab, sat_tab = st.tabs(["日曜日の目標人数", "土曜日の目標人数"])

        with sun_tab:
            c2_1, c2_2, c2_3 = st.columns(3)
            with c2_1: target_pt_sun = st.number_input("PT目標", min_value=0, value=st.session_state.get('pt_sun', 10), step=1, key='pt_sun')
            with c2_2: target_ot_sun = st.number_input("OT目標", min_value=0, value=st.session_state.get('ot_sun', 5), step=1, key='ot_sun')
            with c2_3: target_st_sun = st.number_input("ST目標", min_value=0, value=st.session_state.get('st_sun', 3), step=1, key='st_sun')

        with sat_tab:
            c2_1, c2_2, c2_3 = st.columns(3)
            with c2_1: target_pt_sat = st.number_input("PT目標", min_value=0, value=st.session_state.get('pt_sat', 4), step=1, key='pt_sat', disabled=not is_saturday_special)
            with c2_2: target_ot_sat = st.number_input("OT目標", min_value=0, value=st.session_state.get('ot_sat', 2), step=1, key='ot_sat', disabled=not is_saturday_special)
            with c2_3: target_st_sat = st.number_input("ST目標", min_value=0, value=st.session_state.get('st_sat', 1), step=1, key='st_sat', disabled=not is_saturday_special)
    
        tolerance = st.number_input("PT/OT許容誤差(±)", min_value=0, max_value=5, value=st.session_state.get('tolerance', 1), help="PT/OTの合計人数が目標通りなら、それぞれの人数がこの値までずれてもペナルティを課しません。", key='tolerance')
    
    st.markdown("---")
    st.subheader(f"{year}年{month}月のイベント設定（各日の特別業務単位数を入力）")
    st.info("「全体」タブは職種を問わない業務、「PT/OT/ST」タブは各職種固有の業務を入力します。「全体」に入力された業務は、各職種の標準的な業務量比で自動的に按分されます。")
    
    event_tabs = st.tabs(["全体", "PT", "OT", "ST"])
    event_units_input = {'all': {}, 'pt': {}, 'ot': {}, 'st': {}}
    
    for i, tab_name in enumerate(['all', 'pt', 'ot', 'st']):
        with event_tabs[i]:
            day_counter = 1; num_days_in_month = calendar.monthrange(year, month)[1]; first_day_weekday = calendar.weekday(year, month, 1)
            cal_cols = st.columns(7)
            weekdays_jp = ['月', '火', '水', '木', '金', '土', '日']
            for day_idx, day_name in enumerate(weekdays_jp): cal_cols[day_idx].markdown(f"<p style='text-align: center;'><b>{day_name}</b></p>", unsafe_allow_html=True)
            
            for week_num in range(6):
                cols = st.columns(7)
                for day_of_week in range(7):
                    if (week_num == 0 and day_of_week < first_day_weekday) or day_counter > num_days_in_month:
                        cols[day_of_week].empty(); continue
                    with cols[day_of_week]:
                        is_sunday = calendar.weekday(year, month, day_counter) == 6
                        event_units_input[tab_name][day_counter] = st.number_input(
                            label=f"{day_counter}日", value=0, step=10, disabled=is_sunday, 
                            key=f"event_{tab_name}_{year}_{month}_{day_counter}"
                        )
                    day_counter += 1
                if day_counter > num_days_in_month: break

    st.markdown("---")

with st.expander("▼ ルール検証モード（上級者向け）"):
    params_ui = {}
    st.info("Hルールは必ず守るべき制約、Sルールは努力目標の制約です。ペナルティ値が大きいほど、そのルールが重視されます。")
    st.subheader("H: ハード制約")
    h_cols = st.columns(4)
    h_rules = {'h1':('月間休日数',True,1000), 'h2':('希望休/有休',True,1000), 'h3':('役職者配置',True,1000), 'h5':('週末出勤回数',True,1000)}
    for i, (k, (label, v, p)) in enumerate(h_rules.items()):
        with h_cols[i]: 
            params_ui[k+'_on'] = st.toggle(f'H{i+1}: {label}', value=st.session_state.get(k, v), key=k)
            params_ui[k+'_penalty'] = st.number_input(f"H{i+1} Penalty", value=st.session_state.get(k+'p', p), key=k+'p', disabled=not params_ui[k+'_on'])
    params_ui['h_weekend_limit_penalty'] = params_ui['h5_penalty']
    st.subheader("S: ソフト制約")
    s_cols = st.columns(4)
    s_rules1 = {'s0':('完全週の週休',True,200), 's2':('不完全週の週休',True,25), 's3':('外来同時休',True,10), 's4':('△希望尊重',True,8)}
    for i, (k, (label, v, p)) in enumerate(s_rules1.items()):
        with s_cols[i]: 
            params_ui[k+'_on'] = st.toggle(f'S{i}: {label}', value=st.session_state.get(k, v), key=k)
            params_ui[k+'_penalty'] = st.number_input(f"S{i} Penalty", value=st.session_state.get(k+'p', p), key=k+'p', disabled=not params_ui[k+'_on'])
    s_cols2 = st.columns(4)
    with s_cols2[0]: 
        params_ui['s5_on'] = st.toggle('S5: 回復期配置', value=st.session_state.get('s5', True), key='s5')
        params_ui['s5_penalty'] = st.number_input("S5 Penalty", value=st.session_state.get('s5p', 5), key='s5p', disabled=not params_ui['s5_on'])
    with s_cols2[1]: 
        params_ui['s6_on'] = st.toggle('S6: 業務負荷平準化', value=st.session_state.get('s6', True), key='s6')
        c1,c2 = st.columns(2)
        params_ui['s6_penalty'] = c1.number_input("S6 標準P", value=st.session_state.get('s6p', 2), key='s6p')
        params_ui['s6_penalty_heavy'] = c2.number_input("S6 強化P", value=st.session_state.get('s6ph', 4), key='s6ph')
    with s_cols2[2]: 
        params_ui['s7_on'] = st.toggle('S7: 連続勤務日数', value=st.session_state.get('s7', True), key='s7')
        params_ui['s7_penalty'] = st.number_input("S7 Penalty", value=st.session_state.get('s7p', 50), key='s7p', disabled=not params_ui['s7_on'])
    with s_cols2[3]:
        params_ui['high_flat_penalty'] = st.toggle('平準化ペナルティ強化', value=st.session_state.get('high_flat', False), key='high_flat')
        params_ui['s6_improve_on'] = st.toggle('S6改善: 山登り法', value=st.session_state.get('s6_improve', True), key='s6_improve')
        params_ui['tri_penalty_weight'] = st.number_input('S6改善: △移動コスト', value=st.session_state.get('tri_penalty_weight', 0.5), key='tri_penalty_weight', help='山登り改善で△希望の休日を勤務に変更する際のペナルティコスト。値が大きいほど△が尊重されます。', min_value=0.0, step=0.1)
    st.markdown("##### S1: 日曜人数目標")
    s_cols3 = st.columns(3)
    s_rules2 = {'s1a':('PT/OT合計',True,50), 's1b':('PT/OT個別',True,40), 's1c':('ST目標',True,60)}
    for i, (k, (label, v, p)) in enumerate(s_rules2.items()):
        with s_cols3[i]: 
            params_ui[k+'_on'] = st.toggle(f'S1-{chr(97+i)}: {label}', value=st.session_state.get(k, v), key=k)
            params_ui[k+'_penalty'] = st.number_input(f"S1-{chr(97+i)} Penalty", value=st.session_state.get(k+'p', p), key=k+'p', disabled=not params_ui[k+'_on'])

create_button = st.button('勤務表を作成', type="primary", use_container_width=True)

if create_button:
    if 'confirm_overwrite' in st.session_state and st.session_state.confirm_overwrite:
        st.warning("設定の上書き確認が完了していません。'はい'または'いいえ'を選択してください。")
        st.stop()
    try:
        creds_dict = st.secrets["gcp_service_account"]
        sa = gspread.service_account_from_dict(creds_dict)
        spreadsheet = sa.open("設定ファイル（小野）")
        
        st.info("🔄 スプレッドシートから職員一覧を読み込んでいます...")
        staff_worksheet = spreadsheet.worksheet("職員一覧")
        staff_df = get_as_dataframe(staff_worksheet, dtype={'職員番号': str})
        staff_df.dropna(how='all', inplace=True)

        st.info("🔄 スプレッドシートから希望休一覧を読み込んでいます...")
        requests_worksheet = spreadsheet.worksheet("希望休一覧")
        requests_df = get_as_dataframe(requests_worksheet, dtype={'職員番号': str})
        requests_df.dropna(how='all', inplace=True)
        st.success("✅ データの読み込みが完了しました。")

        params = {}
        params.update(params_ui)
        params['staff_df'] = staff_df
        params['requests_df'] = requests_df
        params['year'] = year; params['month'] = month
        params['tolerance'] = tolerance; params['event_units'] = event_units_input
        
        params['is_saturday_special'] = is_saturday_special
        params['targets'] = {
            'sun': {'pt': target_pt_sun, 'ot': target_ot_sun, 'st': target_st_sun},
            'sat': {'pt': target_pt_sat, 'ot': target_ot_sat, 'st': target_st_sat}
        }
        
        required_staff_cols = ['職員番号', '職種', '1日の単位数', '勤務形態']
        missing_cols = [col for col in required_staff_cols if col not in params['staff_df'].columns]
        if missing_cols:
            st.error(f"エラー: 職員一覧シートの必須列が不足しています: **{', '.join(missing_cols)}**")
            st.stop()

        if '職員番号' not in params['requests_df'].columns:
             st.error(f"エラー: 希望休一覧シートに必須列 **'職員番号'** がありません。")
             st.stop()
        
        if '職員名' not in params['staff_df'].columns:
            params['staff_df']['職員名'] = params['staff_df']['職種'] + " " + params['staff_df']['職員番号'].astype(str)
            st.info("職員一覧に「職員名」列がなかったため、仮の職員名を生成しました。")
        
        is_feasible, schedule_df, summary_df, message, penalty_details = solve_shift_model(params)
        
        st.info(message)
        if is_feasible:
            st.header("勤務表")
            num_days = calendar.monthrange(year, month)[1]
            
            summary_T = summary_df.drop(columns=['日', '曜日']).T
            summary_T.columns = list(range(1, num_days + 1))
            summary_processed = summary_T.reset_index().rename(columns={'index': '職員名'})
            summary_processed['職員番号'] = summary_processed['職員名'].apply(lambda x: f"_{x}")
            summary_processed['職種'] = "サマリー"
            summary_processed = summary_processed[['職員番号', '職員名', '職種'] + list(range(1, num_days + 1))]
            
            # 最終週休日数列を勤務表の最後に結合
            final_df_for_display = pd.concat([schedule_df.drop(columns=['最終週休日数']), summary_processed], ignore_index=True)
            final_df_for_display['最終週休日数'] = schedule_df['最終週休日数'].tolist() + ['' for _ in range(len(summary_processed))]

            days_header = list(range(1, num_days + 1))
            weekdays_header = [ ['月','火','水','木','金','土','日'][calendar.weekday(year, month, d)] for d in days_header]
            final_df_for_display.columns = pd.MultiIndex.from_tuples(
                [('職員情報', '職員番号'), ('職員情報', '職員名'), ('職員情報', '職種')] + 
                list(zip(days_header, weekdays_header)) + 
                [('集計', '最終週休日数')]
            )
            
            # --- ペナルティのハイライトと詳細表示 ---
            styler = final_df_for_display.style.set_properties(**{'text-align': 'center'})

            # 日曜・土曜の背景色
            sunday_cols = [col for col in final_df_for_display.columns if col[1] == '日']
            saturday_cols = [col for col in final_df_for_display.columns if col[1] == '土']
            for col in sunday_cols: styler = styler.set_properties(subset=[col], **{'background-color': '#fff0f0'})
            for col in saturday_cols: styler = styler.set_properties(subset=[col], **{'background-color': '#f0f8ff'})

            if penalty_details:
                # アプローチ2: 表のハイライト
                def highlight_penalties(data):
                    df = data.copy()
                    df.loc[:,:] = '' # デフォルトはスタイルなし

                    for p in penalty_details:
                        day_col_tuples = []
                        if p.get('highlight_days'):
                            for day in p['highlight_days']:
                                try:
                                    weekday_str = weekdays_header[day - 1]
                                    day_col_tuples.append((day, weekday_str))
                                except IndexError:
                                    pass # 日付が範囲外の場合は無視

                        # 職員が特定されているペナルティ
                        if p['staff'] != '-':
                            staff_rows = data[data[('職員情報', '職員名')] == p['staff']].index
                            if not staff_rows.empty:
                                row_idx = staff_rows[0]
                                if day_col_tuples: # 日付が特定されている場合
                                    for day_col_tuple in day_col_tuples:
                                        if day_col_tuple in df.columns:
                                            df.loc[row_idx, day_col_tuple] = 'background-color: #ffcccc'
                                else: # 職員全体にかかるペナルティ (H1, H5など)
                                    df.loc[row_idx, ('職員情報', '職員名')] = 'background-color: #ffcccc'
                        
                        # 職員が特定されていないペナルティ (日付単位)
                        elif day_col_tuples:
                            target_summary_row_name = None
                            if p['rule'] == 'H3: 役職者未配置':
                                target_summary_row_name = '役職者'
                            elif p['rule'] == 'S5: 回復期担当未配置':
                                target_summary_row_name = '回復期'
                            
                            if target_summary_row_name:
                                summary_rows = data[data[('職員情報', '職員名')] == target_summary_row_name].index
                                if not summary_rows.empty:
                                    row_idx = summary_rows[0]
                                    for day_col_tuple in day_col_tuples:
                                        if day_col_tuple in df.columns:
                                            df.loc[row_idx, day_col_tuple] = 'background-color: #ffcccc'

                    return df
                
                styler = styler.apply(highlight_penalties, axis=None)

            st.dataframe(styler)

            # アプローチ1: 詳細リスト
            if penalty_details:
                with st.expander("⚠️ ペナルティ詳細", expanded=True):
                    for p in penalty_details:
                        st.warning(f"**[{p['rule']}]** 職員: {p['staff']} | 日付: {p['day']} | 詳細: {p['detail']}")
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                schedule_df.to_excel(writer, sheet_name='勤務表', index=False)
                summary_df.to_excel(writer, sheet_name='日別サマリー', index=False)
            excel_data = output.getvalue()
            st.download_button(label="📥 Excelでダウンロード", data=excel_data, file_name=f"schedule_{year}{month:02d}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            
    except Exception as e:
        st.error(f'予期せぬエラーが発生しました: {e}')
        st.exception(e)

st.markdown("---")
st.markdown(f"<div style='text-align: right; color: grey;'>{APP_CREDIT} | Version: {APP_VERSION}</div>", unsafe_allow_html=True)
