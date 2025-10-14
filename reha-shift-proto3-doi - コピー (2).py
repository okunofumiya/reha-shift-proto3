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
APP_VERSION = "proto.3.0.0" # 2段階最適化（山登り法）による平準化機能を追加
APP_CREDIT = "Okuno with 🤖 Gemini and Claude"

# --- Gspread ヘルパー関数 (新規追加) ---
@st.cache_resource(ttl=600)
def get_presets_worksheet():
    """Googleスプレッドシートに接続し、'設定プリセット'シートを取得する"""
    try:
        creds_dict = st.secrets["gcp_service_account"]
        sa = gspread.service_account_from_dict(creds_dict)
        spreadsheet = sa.open("設定ファイル（土井）")
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
        'tolerance', 'tri_penalty_weight', 'is_saturday_special',
        'pt_sun', 'ot_sun', 'st_sun', 'pt_sat', 'ot_sat', 'st_sat',
        'h1', 'h1p', 'h2', 'h2p', 'h3', 'h3p', 'h5', 'h5p',
        'h_weekend_limit_penalty',
        's0', 's0p', 's2', 's2p', 's3', 's3p', 's4', 's4p',
        's5', 's5p', 's6', 's6p', 's6ph', 'high_flat', 's7', 's7p',
        's1a', 's1ap', 's1b', 's1bp', 's1c', 's1cp',
        'tri_penalty_weight' # 第1部で追加したキー
    ]
    for key in keys_to_save:
        if key in st.session_state:
            settings[key] = st.session_state[key]
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

# --- 第3部: 山登り法とヘルパー関数 ---
def calculate_internal_penalty_score(shifts_values, params):
    """山登り法が改善方向を判断するための軽量な評価スコアを計算する"""
    total_std_dev = 0
    for job in ['PT', 'OT', 'ST']:
        members = params['job_types'].get(job, [])
        if not members: continue
        daily_counts = [sum(shifts_values.get((s, d), 0) for s in members) for d in params['weekdays']]
        if daily_counts:
            total_std_dev += np.std(daily_counts)
    return total_std_dev

def is_move_valid(temp_shifts_values, staff_id, max_day, min_day, params):
    """山登り法による移動が致命的なルール違反をしないか検証する"""
    staff_info = params['staff_info']
    if staff_info[staff_id].get('勤務形態') == 'パート': return False

    # --- 必須担当者チェック (移動元) ---
    # 役職者
    if pd.notna(staff_info[staff_id]['役職']):
        managers_on_day = sum(temp_shifts_values.get((s, max_day), 0) for s in params.get('managers', []))
        if managers_on_day < 1: return False
    # 回復期担当
    if staff_info[staff_id].get('役割1') == '回復期専従':
        if staff_info[staff_id]['職種'] == '理学療法士':
            kaifukuki_pt_on_day = sum(temp_shifts_values.get((s, max_day), 0) for s in params.get('kaifukuki_pt', []))
            if kaifukuki_pt_on_day < 1: return False
        elif staff_info[staff_id]['職種'] == '作業療法士':
            kaifukuki_ot_on_day = sum(temp_shifts_values.get((s, max_day), 0) for s in params.get('kaifukuki_ot', []))
            if kaifukuki_ot_on_day < 1: return False

    # --- 週休・連勤チェック (対象職員) ---
    s_reqs = params['requests_map'].get(staff_id, {})
    all_half_day_requests = {d for d, r in s_reqs.items() if r in ['AM有', 'PM有', 'AM休', 'PM休']}

    for week in params['weeks_in_month']:
        if not (max_day in week or min_day in week): continue

        num_full_holidays = sum(1 - temp_shifts_values.get((staff_id, d), 0) for d in week)
        num_half_holidays = sum(1 for d in week if d in all_half_day_requests and temp_shifts_values.get((staff_id, d), 0) == 1)
        total_holiday_value = 2 * num_full_holidays + num_half_holidays

        # 月またぎ週の考慮
        is_cross_month_week = params.get('is_cross_month_week', False)
        if is_cross_month_week and week == params['weeks_in_month'][0]:
            prev_week_holidays = staff_info[staff_id].get('前月最終週の休日数', 0) * 2
            total_holiday_value += int(prev_week_holidays)
            if total_holiday_value < 3: return False
        else:
            if len(week) == 7 and total_holiday_value < 3: return False
            if len(week) < 7 and total_holiday_value < 1: return False

    # 連続勤務日数チェック
    max_consecutive_days = 5
    for d in range(1, params['num_days'] + 1):
        # d日目から始まる6日間の勤務をチェック
        if d + max_consecutive_days <= params['num_days']:
            work_days = sum(temp_shifts_values.get((staff_id, d + i), 0) for i in range(max_consecutive_days + 1))
            if work_days > max_consecutive_days: return False
            
    return True

def improve_schedule_with_local_search(shifts_values, params):
    """【週単位改善版】山登り法で平日均等化改善を行う"""
    debug_container = st.expander("山登り法 改善プロセス")
    log_messages = []

    max_iterations = 100 # 無限ループ防止
    for i in range(max_iterations):
        improvement_found_in_pass = False
        current_best_score = calculate_internal_penalty_score(shifts_values, params)

        # 週ごとにループ
        for week_idx, week in enumerate(params['weeks_in_month']):
            week_weekdays = [d for d in week if d in params['weekdays']]
            if len(week_weekdays) < 2: continue

            # 職種ごとにループ
            for job in ['PT', 'OT', 'ST']:
                members = params['job_types'].get(job, [])
                if not members: continue

                daily_counts = {d: sum(shifts_values.get((s, d), 0) for s in members) for d in week_weekdays}
                if not daily_counts: continue

                max_day = max(daily_counts, key=daily_counts.get)
                min_day = min(daily_counts, key=daily_counts.get)

                if daily_counts[max_day] <= daily_counts[min_day] + 1:
                    continue
                
                # 改善候補の職員を探す
                for staff_id in members:
                    is_working_on_max = shifts_values.get((staff_id, max_day), 0) == 1
                    is_off_on_min = shifts_values.get((staff_id, min_day), 0) == 0
                    request_on_min = params['requests_map'].get(staff_id, {}).get(min_day)
                    request_on_max = params['requests_map'].get(staff_id, {}).get(max_day)
                    must_work_symbols = ['○', 'AM休', 'PM休', 'AM有', 'PM有', '出張', '前2h有', '後2h有']

                    if is_working_on_max and is_off_on_min and (request_on_min is None or request_on_min == '△') and (request_on_max not in must_work_symbols):
                        temp_shifts = shifts_values.copy()
                        temp_shifts[(staff_id, max_day)] = 0
                        temp_shifts[(staff_id, min_day)] = 1

                        if not is_move_valid(temp_shifts, staff_id, max_day, min_day, params):
                            continue

                        move_cost = params.get('tri_penalty_weight', 0.5) if request_on_min == '△' else 0
                        new_score = calculate_internal_penalty_score(temp_shifts, params)

                        if new_score + move_cost < current_best_score:
                            staff_name = params['staff_info'][staff_id]['職員名']
                            move_type = "'△'希望" if request_on_min == '△' else "休日"
                            log_messages.append(f"✅ [改善 {len(log_messages)+1}] **{staff_name}**: `{max_day}日` → `{min_day}日` ({move_type}) (スコア: {current_best_score:.4f} → {new_score:.4f})")
                            shifts_values.update(temp_shifts)
                            improvement_found_in_pass = True
                            break # 職員ループ
                if improvement_found_in_pass:
                    break # 職種ループ
            if improvement_found_in_pass:
                break # 週ループ
        
        if not improvement_found_in_pass:
            break 
    
    with debug_container:
        if not log_messages:
            st.info("初期解が既に平準化されているため、山登り法による改善はありませんでした。")
        else:
            st.write(f"{len(log_messages)}回の改善を行いました。")
            for msg in log_messages:
                st.markdown(msg, unsafe_allow_html=True)


# --- 第2部: ペナルティ計算関数 ---
def calculate_final_penalties_and_details(shifts_values, params):
    """完成した勤務表からペナルティスコアと詳細を計算する（S1, S6を含む完全版）"""
    total_penalty = 0
    penalty_details = []
    
    staff_info = params['staff_info']; requests_map = params['requests_map']
    staff = params['staff']; days = params['days']; num_days = len(days)
    weekdays = params['weekdays']; sundays = params['sundays']; special_saturdays = params['special_saturdays']
    job_types = params['job_types']; unit_multiplier_map = params['unit_multiplier_map']
    
    # Hルール群 (基本的な制約)
    # H1: 月間休日数
    if params['h1_on']:
        for s in staff:
            if s in params['part_time_staff_ids']: continue
            num_paid_leave = sum(1 for r in requests_map.get(s, {}).values() if r == '有')
            num_special_leave = sum(1 for r in requests_map.get(s, {}).values() if r == '特')
            num_summer_leave = sum(1 for r in requests_map.get(s, {}).values() if r == '夏')
            num_half_kokyu = sum(1 for r in requests_map.get(s, {}).values() if r in ['AM休', 'PM休'])
            full_holidays_total = sum(1 - shifts_values.get((s, d), 0) for d in days)
            full_holidays_kokyu = full_holidays_total - num_paid_leave - num_special_leave - num_summer_leave
            total_holiday_value = 2 * full_holidays_kokyu + num_half_kokyu
            deviation = abs(total_holiday_value - 18)
            if deviation > 0:
                penalty = params['h1_penalty'] * deviation
                total_penalty += penalty
                penalty_details.append({'rule': 'H1: 月間休日数', 'staff': staff_info[s]['職員名'], 'day': '-', 'detail': f"休日が{total_holiday_value / 2}日分（目標9日）。ペナルティ:{penalty}"})

    # H2, H3, H5, S4, S5, S7... (他の基本ルールは変更なし)
    if params['h2_on']:
        for s, reqs in requests_map.items():
            for d, req_type in reqs.items():
                is_working = shifts_values.get((s, d), 0) == 1
                if req_type in ['×', '有', '特', '夏'] and is_working:
                    total_penalty += params['h2_penalty']
                    penalty_details.append({'rule': 'H2: 希望休違反', 'staff': staff_info[s]['職員名'], 'day': d, 'detail': f"{d}日の「{req_type}」希望に反して出勤。"})
                elif req_type in ['○', 'AM有', 'PM有', 'AM休', 'PM休', '出張', '前2h有', '後2h有'] and not is_working:
                    total_penalty += params['h2_penalty']
                    penalty_details.append({'rule': 'H2: 希望休違反', 'staff': staff_info[s]['職員名'], 'day': d, 'detail': f"{d}日の「{req_type}」希望に反して休み。"})
    if params['h3_on']:
        for d in days:
            if sum(shifts_values.get((s, d), 0) for s in params['managers']) == 0:
                total_penalty += params['h3_penalty']
                penalty_details.append({'rule': 'H3: 役職者未配置', 'staff': '-', 'day': d, 'detail': f"{d}日に役職者不在。"})
    if params.get('h5_on', False):
        for s in staff:
            if s in params['part_time_staff_ids']: continue
            sun_sat_limit = pd.to_numeric(staff_info[s].get('土日上限'), errors='coerce')
            sun_sat_lower_limit = pd.to_numeric(staff_info[s].get('土日下限'), errors='coerce')
            num_sun_sat_worked = sum(shifts_values.get((s, d), 0) for d in sundays + special_saturdays)
            if pd.notna(sun_sat_limit) and num_sun_sat_worked > sun_sat_limit:
                over = num_sun_sat_worked - sun_sat_limit
                total_penalty += params['h5_penalty'] * over
                penalty_details.append({'rule': 'H5: 土日出勤上限超', 'staff': staff_info[s]['職員名'], 'day': '-', 'detail': f"土日合計出勤が{num_sun_sat_worked}回で上限({sun_sat_limit})超。"})
            if pd.notna(sun_sat_lower_limit) and num_sun_sat_worked < sun_sat_lower_limit:
                under = sun_sat_lower_limit - num_sun_sat_worked
                total_penalty += params['h5_penalty'] * under
                penalty_details.append({'rule': 'H5: 土日出勤下限未達', 'staff': staff_info[s]['職員名'], 'day': '-', 'detail': f"土日合計出勤が{num_sun_sat_worked}回で下限({sun_sat_lower_limit})未達。"})

    # Sルール群 (ソフト制約)
    # S1: 週末人数目標 (完全実装)
    if any([params['s1a_on'], params['s1b_on'], params['s1c_on']]):
        special_days_map = {'sun': sundays}
        if special_saturdays: special_days_map['sat'] = special_saturdays
        for day_type, special_days in special_days_map.items():
            target_pt = params['targets'][day_type]['pt']; target_ot = params['targets'][day_type]['ot']; target_st = params['targets'][day_type]['st']
            for d in special_days:
                pt_on_day = sum(shifts_values.get((s, d), 0) for s in job_types['PT'])
                ot_on_day = sum(shifts_values.get((s, d), 0) for s in job_types['OT'])
                st_on_day = sum(shifts_values.get((s, d), 0) for s in job_types['ST'])
                if params['s1a_on']:
                    total_diff = abs((pt_on_day + ot_on_day) - (target_pt + target_ot))
                    total_penalty += params['s1a_penalty'] * total_diff
                if params['s1b_on']:
                    pt_penalty = max(0, abs(pt_on_day - target_pt) - params['tolerance'])
                    ot_penalty = max(0, abs(ot_on_day - target_ot) - params['tolerance'])
                    total_penalty += params['s1b_penalty'] * (pt_penalty + ot_penalty)
                if params['s1c_on']:
                    st_diff = abs(st_on_day - target_st)
                    total_penalty += params['s1c_penalty'] * st_diff

    # S6: 業務負荷平準化 (完全実装)
    if params['s6_on']:
        unit_penalty_weight = params.get('s6_penalty_heavy', 4) if params.get('high_flat_penalty') else params.get('s6_penalty', 2)
        event_units = params['event_units']
        
        total_weekday_units_by_job = {}
        for job, members in job_types.items():
            if not members or not weekdays: total_weekday_units_by_job[job] = 0; continue
            total_units = sum(int(staff_info[s]['1日の単位数']) * (1 - sum(1 for d in weekdays if requests_map.get(s, {}).get(d) in ['有','特','夏','×','△']) / len(weekdays)) for s in members)
            total_weekday_units_by_job[job] = total_units
        
        total_all_jobs_units = sum(total_weekday_units_by_job.values())
        ratios = {job: total_units / total_all_jobs_units if total_all_jobs_units > 0 else 0 for job, total_units in total_weekday_units_by_job.items()}
        
        avg_residual_units_by_job = {}
        total_event_units_all = sum(event_units['all'].get(d, 0) for d in weekdays)
        for job, members in job_types.items():
            if not weekdays or not members: avg_residual_units_by_job[job] = 0; continue
            total_event_units_job = sum(event_units[job.lower()].get(d, 0) for d in weekdays)
            total_event_units_for_job = total_event_units_job + (total_event_units_all * ratios.get(job, 0))
            avg_residual_units_by_job[job] = (total_weekday_units_by_job.get(job, 0) - total_event_units_for_job) / len(weekdays)

        for job, members in job_types.items():
            if not members: continue
            avg_residual_units = avg_residual_units_by_job.get(job, 0); ratio = ratios.get(job, 0)
            for d in weekdays:
                provided_units = sum(shifts_values.get((s, d), 0) * int(int(staff_info[s]['1日の単位数']) * unit_multiplier_map.get(s, {}).get(d, 1.0)) for s in members)
                event_unit_for_day = event_units[job.lower()].get(d, 0) + (event_units['all'].get(d, 0) * ratio)
                residual_units = provided_units - event_unit_for_day
                diff = abs(residual_units - avg_residual_units)
                total_penalty += unit_penalty_weight * diff

    return total_penalty, penalty_details

# --- メインのソルバー関数 ---
def solve_shift_model(params):
    year, month = params['year'], params['month']
    num_days = calendar.monthrange(year, month)[1]; days = list(range(1, num_days + 1))
    params['num_days'] = num_days
    
    staff = params['staff_df']['職員番号'].tolist()
    staff_info = params['staff_df'].set_index('職員番号').to_dict('index')
    params['staff_info'] = staff_info 
    params['staff'] = staff 

    part_time_staff_ids = [s for s in staff if staff_info[s].get('勤務形態') == 'パート']
    params['part_time_staff_ids'] = part_time_staff_ids 

    sundays = [d for d in days if calendar.weekday(year, month, d) == 6]
    saturdays = [d for d in days if calendar.weekday(year, month, d) == 5]
    special_saturdays = saturdays if params.get('is_saturday_special', False) else []
    weekdays = [d for d in days if d not in sundays and d not in special_saturdays]
    params['sundays'] = sundays; params['special_saturdays'] = special_saturdays
    params['weekdays'] = weekdays; params['days'] = days 
    
    managers = [s for s in staff if pd.notna(staff_info[s]['役職'])]; pt_staff = [s for s in staff if staff_info[s]['職種'] == '理学療法士']
    ot_staff = [s for s in staff if staff_info[s]['職種'] == '作業療法士']; st_staff = [s for s in staff if staff_info[s]['職種'] == '言語聴覚士']
    params['managers'] = managers; params['pt_staff'] = pt_staff; params['ot_staff'] = ot_staff; params['st_staff'] = st_staff 
    
    kaifukuki_staff = [s for s in staff if staff_info[s].get('役割1') == '回復期専従']; kaifukuki_pt = [s for s in kaifukuki_staff if staff_info[s]['職種'] == '理学療法士']
    kaifukuki_ot = [s for s in kaifukuki_staff if staff_info[s]['職種'] == '作業療法士']; gairai_staff = [s for s in staff if staff_info[s].get('役割1') == '外来PT']
    chiiki_staff = [s for s in staff if staff_info[s].get('役割1') == '地域包括専従']
    params['kaifukuki_pt'] = kaifukuki_pt; params['kaifukuki_ot'] = kaifukuki_ot; params['gairai_staff'] = gairai_staff 
    job_types = {'PT': pt_staff, 'OT': ot_staff, 'ST': st_staff}
    params['job_types'] = job_types 
    
    # --- 希望休と単位数倍率のマップを作成 ---
    requests_map = {s: {} for s in staff}
    unit_multiplier_map = {s: {} for s in staff}
    for index, row in params['requests_df'].iterrows():
        staff_id = row['職員番号']
        if staff_id not in staff: continue
        for d in days:
            col_name = str(d)
            if col_name in row and pd.notna(row[col_name]):
                req = row[col_name]
                requests_map[staff_id][d] = req
                # 単位数倍率を設定
                if req in ['AM休', 'PM休', 'AM有', 'PM有']:
                    unit_multiplier_map[staff_id][d] = 0.5
                elif req == '出張':
                    unit_multiplier_map[staff_id][d] = 0.0
                elif req in ['前2h有', '後2h有']:
                    unit_multiplier_map[staff_id][d] = 0.7
                else:
                    unit_multiplier_map[staff_id][d] = 1.0 # 通常の出勤

    params['requests_map'] = requests_map
    params['unit_multiplier_map'] = unit_multiplier_map

    # --- 月またぎ週の判定 ---
    prev_month_date = datetime(year, month, 1) - relativedelta(days=1)
    is_cross_month_week = prev_month_date.weekday() != 5 # 5: Saturday
    params['is_cross_month_week'] = is_cross_month_week

    # --- 前月最終週の休日数をスタッフ情報にマージ ---
    if is_cross_month_week and '前月最終週の休日数' in params['requests_df'].columns:
        staff_df_merged = params['staff_df'].merge(params['requests_df'][['職員番号', '前月最終週の休日数']], on='職員番号', how='left')
        staff_df_merged['前月最終週の休日数'].fillna(0, inplace=True)
        params['staff_info'] = staff_df_merged.set_index('職員番号').to_dict('index')
        staff_info = params['staff_info']
    else:
        # マージしない場合も、キーが存在するようにデフォルト値0を設定
        for s_info in staff_info.values():
            s_info['前月最終週の休日数'] = 0

    weeks_in_month = []; current_week = []
    for d in days:
        current_week.append(d)
        if calendar.weekday(year, month, d) == 5 or d == num_days: weeks_in_month.append(current_week); current_week = []
    params['weeks_in_month'] = weeks_in_month

    model = cp_model.CpModel(); shifts = {}
    for s in staff:
        for d in days: shifts[(s, d)] = model.NewBoolVar(f'shift_{s}_{d}')

    penalties = []
    
    # (略) ... CP-SATの制約定義は変更なし ...
    if params['h1_on']:
        for s_idx, s in enumerate(staff):
            if s in params['part_time_staff_ids']: continue
            s_reqs = requests_map.get(s, {})
            num_paid_leave = sum(1 for r in s_reqs.values() if r == '有')
            num_special_leave = sum(1 for r in s_reqs.values() if r == '特')
            num_summer_leave = sum(1 for r in s_reqs.values() if r == '夏')
            num_half_kokyu = sum(1 for r in s_reqs.values() if r in ['AM休', 'PM休'])
            
            full_holidays_total = sum(1 - shifts[(s, d)] for d in days)
            full_holidays_kokyu = model.NewIntVar(0, num_days, f'full_kokyu_{s}')
            model.Add(full_holidays_kokyu == full_holidays_total - num_paid_leave - num_special_leave - num_summer_leave)
            
            total_holiday_value = model.NewIntVar(0, num_days * 2, f'total_holiday_value_{s}')
            model.Add(total_holiday_value == 2 * full_holidays_kokyu + num_half_kokyu)
            
            deviation = model.NewIntVar(-num_days * 2, num_days * 2, f'h1_dev_{s}')
            model.Add(deviation == total_holiday_value - 18)
            
            abs_deviation = model.NewIntVar(0, num_days * 2, f'h1_abs_dev_{s}')
            model.AddAbsEquality(abs_deviation, deviation)
            penalties.append(params['h1_penalty'] * abs_deviation)

    if params['h2_on']:
        for s, reqs in requests_map.items():
            for d, req_type in reqs.items():
                if s in params['part_time_staff_ids']:
                    if req_type == '×' or req_type == '有': model.Add(shifts[(s, d)] == 0)
                    else: model.Add(shifts[(s, d)] == 1)
                else:
                    if req_type in ['×', '有', '特', '夏']:
                        penalties.append(params['h2_penalty'] * shifts[(s, d)])
                    elif req_type in ['○', 'AM有', 'PM有', 'AM休', 'PM休', '出張', '前2h有', '後2h有']:
                        penalties.append(params['h2_penalty'] * (1 - shifts[(s, d)]))

    if params['h3_on']:
        for d in days:
            no_manager = model.NewBoolVar(f'no_manager_{d}')
            model.Add(sum(shifts[(s, d)] for s in managers) == 0).OnlyEnforceIf(no_manager)
            model.Add(sum(shifts[(s, d)] for s in managers) > 0).OnlyEnforceIf(no_manager.Not())
            penalties.append(params['h3_penalty'] * no_manager)
    
    if params.get('h5_on', False):
        for s in staff:
            if s in params['part_time_staff_ids']: continue
            sun_sat_limit = pd.to_numeric(staff_info[s].get('土日上限'), errors='coerce')
            sun_sat_lower_limit = pd.to_numeric(staff_info[s].get('土日下限'), errors='coerce')
            if pd.notna(sun_sat_limit):
                num_sun_sat_worked = sum(shifts[(s, d)] for d in sundays + special_saturdays)
                over_limit = model.NewIntVar(0, len(sundays) + len(special_saturdays), f'sun_sat_over_{s}')
                model.Add(over_limit >= num_sun_sat_worked - int(sun_sat_limit))
                penalties.append(params['h5_penalty'] * over_limit)
            if pd.notna(sun_sat_lower_limit):
                num_sun_sat_worked = sum(shifts[(s, d)] for d in sundays + special_saturdays)
                under_limit = model.NewIntVar(0, len(sundays) + len(special_saturdays), f'sun_sat_under_{s}')
                model.Add(under_limit >= int(sun_sat_lower_limit) - num_sun_sat_worked)
                penalties.append(params['h5_penalty'] * under_limit)

    if params['s4_on']:
        for s, reqs in requests_map.items():
            for d, req_type in reqs.items():
                if req_type == '△':
                    penalties.append(params['s4_penalty'] * shifts[(s, d)])

    if params['s0_on'] or params['s2_on']:
        for s_idx, s in enumerate(staff):
            if s in params['part_time_staff_ids']: continue
            s_reqs = requests_map.get(s, {})
            all_half_day_requests = {d for d, r in s_reqs.items() if r in ['AM有', 'PM有', 'AM休', 'PM休']}
            for w_idx, week in enumerate(weeks_in_month):
                num_full_holidays_in_week = sum(1 - shifts[(s, d)] for d in week)
                num_half_holidays_in_week = sum(shifts[(s, d)] for d in week if d in all_half_day_requests)
                total_holiday_value = model.NewIntVar(0, 28, f'thv_s{s_idx}_w{w_idx}')
                model.Add(total_holiday_value == 2 * num_full_holidays_in_week + num_half_holidays_in_week)
                if is_cross_month_week and w_idx == 0:
                    prev_week_holidays = staff_info[s].get('前月最終週の休日数', 0) * 2
                    model.Add(total_holiday_value + int(prev_week_holidays) >= 3)
                else:
                    if len(week) == 7 and params['s0_on']:
                        model.Add(total_holiday_value >= 3)
                    elif len(week) < 7 and params['s2_on']:
                        model.Add(total_holiday_value >= 1)

    if params['s5_on']:
        for d in days:
            model.Add(sum(shifts[(s, d)] for s in kaifukuki_pt) >= 1)
            model.Add(sum(shifts[(s, d)] for s in kaifukuki_ot) >= 1)

    if params.get('s7_on', False):
        max_consecutive_days = 5
        for s in staff:
            if s in params['part_time_staff_ids']: continue
            for d in range(1, num_days - max_consecutive_days + 1):
                model.Add(sum(shifts[(s, d + i)] for i in range(max_consecutive_days + 1)) <= max_consecutive_days)
    # (略) ... その他の制約定義 ...
    if any([params['s1a_on'], params['s1b_on'], params['s1c_on']]):
        special_days_map = {'sun': sundays}
        if special_saturdays: special_days_map['sat'] = special_saturdays

        for day_type, special_days in special_days_map.items():
            target_pt = params['targets'][day_type]['pt']; target_ot = params['targets'][day_type]['ot']; target_st = params['targets'][day_type]['st']
            for d in special_days:
                pt_on_day = sum(shifts[(s, d)] for s in pt_staff); ot_on_day = sum(shifts[(s, d)] for s in ot_staff); st_on_day = sum(shifts[(s, d)] for s in st_staff)
                if params['s1a_on']:
                    total_pt_ot = pt_on_day + ot_on_day; total_diff = model.NewIntVar(-50, 50, f't_d_{day_type}_{d}'); model.Add(total_diff == total_pt_ot - (target_pt + target_ot)); abs_total_diff = model.NewIntVar(0, 50, f'a_t_d_{day_type}_{d}'); model.AddAbsEquality(abs_total_diff, total_diff); penalties.append(params['s1a_penalty'] * abs_total_diff)
                if params['s1b_on']:
                    pt_diff = model.NewIntVar(-30, 30, f'p_d_{day_type}_{d}'); model.Add(pt_diff == pt_on_day - target_pt); pt_penalty = model.NewIntVar(0, 30, f'p_p_{day_type}_{d}'); model.Add(pt_penalty >= pt_diff - params['tolerance']); model.Add(pt_penalty >= -pt_diff - params['tolerance']); penalties.append(params['s1b_penalty'] * pt_penalty)
                    ot_diff = model.NewIntVar(-30, 30, f'o_d_{day_type}_{d}'); model.Add(ot_diff == ot_on_day - target_ot); ot_penalty = model.NewIntVar(0, 30, f'o_p_{day_type}_{d}'); model.Add(ot_penalty >= ot_diff - params['tolerance']); model.Add(ot_penalty >= -ot_diff - params['tolerance']); penalties.append(params['s1b_penalty'] * ot_penalty)
                if params['s1c_on']:
                    st_diff = model.NewIntVar(-10, 10, f's_d_{day_type}_{d}'); model.Add(st_diff == st_on_day - target_st); abs_st_diff = model.NewIntVar(0, 10, f'a_s_d_{day_type}_{d}'); model.AddAbsEquality(abs_st_diff, st_diff); penalties.append(params['s1c_penalty'] * abs_st_diff)
    if params['s6_on']:
        unit_penalty_weight = params.get('s6_penalty_heavy', 4) if params.get('high_flat_penalty') else params.get('s6_penalty', 2)
        event_units = params['event_units']
        unit_multiplier_map = params['unit_multiplier_map']
        total_weekday_units_by_job = {}
        for job, members in job_types.items():
            if not members: total_weekday_units_by_job[job] = 0; continue
            total_units = sum(int(staff_info[s]['1日の単位数']) * (1 - sum(1 for d in weekdays if requests_map.get(s, {}).get(d) in ['有','特','夏','×','△']) / len(weekdays)) for s in members)
            total_weekday_units_by_job[job] = total_units
        total_all_jobs_units = sum(total_weekday_units_by_job.values())
        ratios = {job: total_units / total_all_jobs_units if total_all_jobs_units > 0 else 0 for job, total_units in total_weekday_units_by_job.items()}
        avg_residual_units_by_job = {}
        total_event_units_all = sum(event_units['all'].values())
        for job, members in job_types.items():
            if not weekdays or not members: avg_residual_units_by_job[job] = 0; continue
            total_event_units_job = sum(event_units[job.lower()].values())
            total_event_units_for_job = total_event_units_job + (total_event_units_all * ratios.get(job, 0))
            avg_residual_units_by_job[job] = (total_weekday_units_by_job.get(job, 0) - total_event_units_for_job) / len(weekdays)
        for job, members in job_types.items():
            if not members: continue
            avg_residual_units = avg_residual_units_by_job.get(job, 0); ratio = ratios.get(job, 0)
            for d in weekdays:
                provided_units_expr = sum(shifts[(s,d)] * int(int(staff_info[s]['1日の単位数']) * unit_multiplier_map.get(s, {}).get(d, 1.0)) for s in members)
                event_unit_for_day = event_units[job.lower()].get(d, 0) + (event_units['all'].get(d, 0) * ratio)
                residual_units_expr = provided_units_expr - round(event_unit_for_day)
                diff_expr = model.NewIntVar(-4000, 4000, f'u_d_{job}_{d}'); model.Add(diff_expr == residual_units_expr - round(avg_residual_units))
                abs_diff_expr = model.NewIntVar(0, 4000, f'a_u_d_{job}_{d}'); model.AddAbsEquality(abs_diff_expr, diff_expr); penalties.append(unit_penalty_weight * abs_diff_expr)


    model.Minimize(sum(penalties))
    solver = cp_model.CpSolver()
    import random
    solver.parameters.random_seed = random.randint(0, 2**30)
    solver.parameters.max_time_in_seconds = 60.0
    status = solver.Solve(model)

    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        # --- 2段階最適化フロー ---
        # 1. 初期解を生成
        shifts_values = {(s, d): solver.Value(shifts[(s, d)]) for s in staff for d in days}

        # 2. 改善前の品質を評価
        initial_score, _ = calculate_final_penalties_and_details(shifts_values, params)

        # 3. 山登り法で改善
        improve_schedule_with_local_search(shifts_values, params)

        # 4. 改善後の品質を評価
        final_score, final_details = calculate_final_penalties_and_details(shifts_values, params)
        
        # 5. メッセージ作成と結果返却
        message = f"求解ステータス: **{solver.StatusName(status)}** | 改善前スコア: **{round(initial_score)}** → 最終スコア: **{round(final_score)}** (改善量: **{round(initial_score - final_score)}**)"
        
        schedule_df = _create_schedule_df(shifts_values, staff, days, params['staff_df'], requests_map, year, month)
        summary_df = _create_summary(schedule_df, staff_info, year, month, params['event_units'], params['unit_multiplier_map'])
        
        return True, schedule_df, summary_df, message, final_details
    else:
        message = f"致命的なエラー: ハード制約が矛盾しているため、勤務表を作成できませんでした。({solver.StatusName(status)})"
        return False, pd.DataFrame(), pd.DataFrame(), message, []

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
    st.warning("注意: 各ルールのON/OFFやペナルティ値を変更することで、意図しない結果や、解が見つからない状況が発生する可能性があります。")
    st.markdown("---")
    st.subheader("基本ルール（違反時にペナルティが発生）")
    st.info("これらのルールは通常ONですが、どうしても解が見つからない場合にOFFにできます。")
    h_cols = st.columns(4)
    params_ui = {}
    with h_cols[0]:
        params_ui['h1_on'] = st.toggle('H1: 月間休日数', value=st.session_state.get('h1', True), key='h1')
        params_ui['h1_penalty'] = st.number_input("H1 Penalty", value=st.session_state.get('h1p', 1000), disabled=not params_ui['h1_on'], key='h1p')
    with h_cols[1]:
        params_ui['h2_on'] = st.toggle('H2: 希望休/有休', value=st.session_state.get('h2', True), key='h2')
        params_ui['h2_penalty'] = st.number_input("H2 Penalty", value=st.session_state.get('h2p', 1000), disabled=not params_ui['h2_on'], key='h2p')
    with h_cols[2]:
        params_ui['h3_on'] = st.toggle('H3: 役職者配置', value=st.session_state.get('h3', True), key='h3')
        params_ui['h3_penalty'] = st.number_input("H3 Penalty", value=st.session_state.get('h3p', 1000), disabled=not params_ui['h3_on'], key='h3p')
    with h_cols[3]:
        params_ui['h5_on'] = st.toggle('H5: 土日出勤回数', value=st.session_state.get('h5', True), key='h5', help="職員ごとに設定された土日の出勤回数の上限/下限を守るルールです。")
        params_ui['h5_penalty'] = st.number_input("H5 Penalty", value=st.session_state.get('h5p', 1000), disabled=not params_ui['h5_on'], key='h5p')
    
    params_ui['h_weekend_limit_penalty'] = params_ui['h5_penalty'] # 互換性のための代入
    
    params_ui['h4_on'] = False
    st.markdown("---")
    st.subheader("ソフト制約のON/OFFとペナルティ設定")
    st.info("S0/S2の週休ルールは、半日休を0.5日分の休みとしてカウントし、完全な週は1.5日以上、不完全な週は0.5日以上の休日確保を目指します。")
    s_cols = st.columns(4)
    with s_cols[0]:
        params_ui['s0_on'] = st.toggle('S0: 完全週の週休1.5日', value=st.session_state.get('s0', True), key='s0')
        params_ui['s0_penalty'] = st.number_input("S0 Penalty", value=st.session_state.get('s0p', 200), disabled=not params_ui['s0_on'], key='s0p')
    with s_cols[1]:
        params_ui['s2_on'] = st.toggle('S2: 不完全週の週休0.5日', value=st.session_state.get('s2', True), key='s2')
        params_ui['s2_penalty'] = st.number_input("S2 Penalty", value=st.session_state.get('s2p', 25), disabled=not params_ui['s2_on'], key='s2p')
    with s_cols[2]:
        params_ui['s3_on'] = st.toggle('S3: 外来同時休', value=st.session_state.get('s3', True), key='s3')
        params_ui['s3_penalty'] = st.number_input("S3 Penalty", value=st.session_state.get('s3p', 10), disabled=not params_ui['s3_on'], key='s3p')
    with s_cols[3]:
        params_ui['s4_on'] = st.toggle('S4: 準希望休(△)尊重', value=st.session_state.get('s4', True), key='s4')
        params_ui['s4_penalty'] = st.number_input("S4 Penalty", value=st.session_state.get('s4p', 8), help="値が大きいほど△希望が尊重されます。", disabled=not params_ui['s4_on'], key='s4p')
    s_cols2 = st.columns(4)
    with s_cols2[0]:
        params_ui['s5_on'] = st.toggle('S5: 回復期配置', value=st.session_state.get('s5', True), key='s5')
        params_ui['s5_penalty'] = st.number_input("S5 Penalty", value=st.session_state.get('s5p', 5), disabled=not params_ui['s5_on'], key='s5p')
    with s_cols2[1]:
        params_ui['s6_on'] = st.toggle('S6: 職種別 業務負荷平準化', value=st.session_state.get('s6', True), key='s6')
        c_s6_1, c_s6_2 = st.columns(2)
        params_ui['s6_penalty'] = c_s6_1.number_input("S6 標準P", value=st.session_state.get('s6p', 2), disabled=not params_ui['s6_on'], key='s6p')
        params_ui['s6_penalty_heavy'] = c_s6_2.number_input("S6 強化P", value=st.session_state.get('s6ph', 4), disabled=not params_ui['s6_on'], key='s6ph')
    with s_cols2[2]:
        params_ui['s7_on'] = st.toggle('S7: 連続勤務日数', value=st.session_state.get('s7', True), key='s7')
        params_ui['s7_penalty'] = st.number_input("S7 Penalty", value=st.session_state.get('s7p', 50), disabled=not params_ui['s7_on'], key='s7p')
    with s_cols2[3]:
        params_ui['high_flat_penalty'] = st.toggle('平準化ペナルティ強化', value=st.session_state.get('high_flat', False), key='high_flat', help="S6のペナルティを「標準P」ではなく「強化P」で計算します。")
        # --- 第1部: UIの修正 ---
        params_ui['tri_penalty_weight'] = st.number_input(
            'S6改善: △移動コスト', 
            min_value=0.0, 
            value=st.session_state.get('tri_penalty_weight', 0.5), 
            step=0.1, 
            key='tri_penalty_weight',
            help='山登り改善で△希望の休日を勤務に変更する際のペナルティコスト。値が大きいほど△が尊重されます。'
        )

    st.markdown("##### S1: 日曜人数目標")
    s_cols3 = st.columns(3)
    with s_cols3[0]:
        params_ui['s1a_on'] = st.toggle('S1-a: PT/OT合計', value=st.session_state.get('s1a', True), key='s1a')
        params_ui['s1a_penalty'] = st.number_input("S1-a Penalty", value=st.session_state.get('s1ap', 50), disabled=not params_ui['s1a_on'], key='s1ap')
    with s_cols3[1]:
        params_ui['s1b_on'] = st.toggle('S1-b: PT/OT個別', value=st.session_state.get('s1b', True), key='s1b')
        params_ui['s1b_penalty'] = st.number_input("S1-b Penalty", value=st.session_state.get('s1bp', 40), disabled=not params_ui['s1b_on'], key='s1bp')
    with s_cols3[2]:
        params_ui['s1c_on'] = st.toggle('S1-c: ST目標', value=st.session_state.get('s1c', True), key='s1c')
        params_ui['s1c_penalty'] = st.number_input("S1-c Penalty", value=st.session_state.get('s1cp', 60), disabled=not params_ui['s1c_on'], key='s1cp')

create_button = st.button('勤務表を作成', type="primary", use_container_width=True)

if create_button:
    if 'confirm_overwrite' in st.session_state and st.session_state.confirm_overwrite:
        st.warning("設定の上書き確認が完了していません。'はい'または'いいえ'を選択してください。")
        st.stop()
    try:
        creds_dict = st.secrets["gcp_service_account"]
        sa = gspread.service_account_from_dict(creds_dict)
        spreadsheet = sa.open("設定ファイル（土井）")
        
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
            
            final_df_for_display = pd.concat([schedule_df.drop(columns=['最終週休日数']), summary_processed], ignore_index=True)
            final_df_for_display['最終週休日数'] = schedule_df['最終週休日数'].tolist() + ['' for _ in range(len(summary_processed))]

            days_header = list(range(1, num_days + 1))
            weekdays_header = [ ['月','火','水','木','金','土','日'][calendar.weekday(year, month, d)] for d in days_header]
            final_df_for_display.columns = pd.MultiIndex.from_tuples(
                [('職員情報', '職員番号'), ('職員情報', '職員名'), ('職員情報', '職種')] + 
                list(zip(days_header, weekdays_header)) + 
                [('集計', '最終週休日数')]
            )
            
            styler = final_df_for_display.style.set_properties(**{'text-align': 'center'})

            sunday_cols = [col for col in final_df_for_display.columns if col[1] == '日']
            saturday_cols = [col for col in final_df_for_display.columns if col[1] == '土']
            for col in sunday_cols: styler = styler.set_properties(subset=[col], **{'background-color': '#fff0f0'})
            for col in saturday_cols: styler = styler.set_properties(subset=[col], **{'background-color': '#f0f8ff'})

            if penalty_details:
                def highlight_penalties(data):
                    df = data.copy(); df.loc[:,:] = ''
                    for p in penalty_details:
                        day_col_tuples = []
                        if isinstance(p.get('day'), int):
                            day = p['day']
                            weekday_str = weekdays_header[day - 1]
                            day_col_tuples.append((day, weekday_str))
                        
                        if p['staff'] != '-':
                            staff_rows = data[data[('職員情報', '職員名')] == p['staff']].index
                            if not staff_rows.empty:
                                row_idx = staff_rows[0]
                                if day_col_tuples:
                                    for day_col_tuple in day_col_tuples:
                                        if day_col_tuple in df.columns:
                                            df.loc[row_idx, day_col_tuple] = 'background-color: #ffcccc'
                                else:
                                    df.loc[row_idx, ('職員情報', '職員名')] = 'background-color: #ffcccc'
                    return df
                styler = styler.apply(highlight_penalties, axis=None)

            st.dataframe(styler)

            if penalty_details:
                with st.expander("⚠️ ペナルティ詳細（改善後）", expanded=True):
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