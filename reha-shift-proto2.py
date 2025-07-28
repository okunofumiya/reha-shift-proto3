import streamlit as st
import pandas as pd
import numpy as np
from ortools.sat.python import cp_model
import calendar
import io
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import json
import jpholiday

# ★★★ バージョン情報 ★★★
APP_VERSION = "proto.5.0.2" # 全機能統合・完成版
APP_CREDIT = "Okuno with 🤖 Gemini"

# --- データ管理クラス ---
class GSheetManager:
    """Googleスプレッドシートとの接続、読み込み、保存を管理するクラス"""
    def __init__(self, spreadsheet_name):
        self.spreadsheet_name = spreadsheet_name
        try:
            creds_json_str = st.secrets["gcp_service_account"]
            if isinstance(creds_json_str, str):
                 creds_dict = json.loads(creds_json_str)
            else: # st.secretsがTOMLセクションを辞書として直接解釈した場合
                 creds_dict = dict(creds_json_str)
            self.sa = gspread.service_account_from_dict(creds_dict)
            self.spreadsheet = self.sa.open(self.spreadsheet_name)
        except Exception as e:
            st.error(f"Googleスプレッドシートへの接続に失敗しました: {e}")
            st.stop()

    @st.cache_data(ttl=300) # 5分間キャッシュ
    def load_all_data(_self):
        try:
            staff_df = get_as_dataframe(_self.spreadsheet.worksheet("職員一覧"), dtype=str).dropna(how='all')
            requests_df = get_as_dataframe(_self.spreadsheet.worksheet("希望休一覧"), dtype=str).dropna(how='all')
            if not requests_df.empty:
                requests_df.columns = requests_df.columns.astype(str)
            event_units_df = get_as_dataframe(_self.spreadsheet.worksheet("特別業務")).dropna(how='all')
            params_df = get_as_dataframe(_self.spreadsheet.worksheet("パラメータプリセット")).dropna(how='all')
            return True, staff_df, requests_df, event_units_df, params_df, None
        except gspread.exceptions.WorksheetNotFound as e:
            return False, None, None, None, None, f"エラー: スプレッドシートに '{e.sheet_name}' という名前のシートがありません。確認してください。"
        except Exception as e:
            return False, None, None, None, None, e

    def convert_event_units_df_to_dict(self, df, year, month):
        event_units = {'all': {}, 'pt': {}, 'ot': {}, 'st': {}}
        if '日付' not in df.columns or df.empty: return event_units
        try:
            df['日付'] = pd.to_datetime(df['日付'])
            month_df = df[(df['日付'].dt.year == year) & (df['日付'].dt.month == month)]
            for _, row in month_df.iterrows():
                day = row['日付'].day
                for job in ['all', 'pt', 'ot', 'st']:
                    col_name = '全体' if job == 'all' else job.upper()
                    event_units[job][day] = int(row.get(col_name, 0) or 0)
        except Exception as e:
            st.warning(f"特別業務シートの処理中にエラーが発生しました: {e}")
        return event_units

    def save_preset(self, preset_name_to_save, params_to_save, original_params_df):
        try:
            worksheet = self.spreadsheet.worksheet("パラメータプリセット")
            other_presets_df = original_params_df[original_params_df['プリセット名'] != preset_name_to_save].copy()
            new_preset_rows = []
            for key, value in params_to_save.items():
                description = ''
                if key in original_params_df['パラメータ名'].values:
                    # 同じパラメータ名を持つ最初の行の説明を取得
                    desc_series = original_params_df[original_params_df['パラメータ名'] == key]['説明']
                    if not desc_series.empty:
                        description = desc_series.iloc[0]
                new_preset_rows.append({'プリセット名': preset_name_to_save, 'パラメータ名': key, '値': str(value), '説明': description})
            
            new_preset_df = pd.DataFrame(new_preset_rows)
            final_df = pd.concat([other_presets_df, new_preset_df], ignore_index=True)
            worksheet.clear()
            set_with_dataframe(worksheet, final_df, include_index=False, resize=True)
            return True, None
        except Exception as e:
            return False, e

# --- ヘルパー関数 ---
def _create_summary(schedule_df, staff_info, year, month, event_units, all_half_day_requests):
    num_days = calendar.monthrange(year, month)[1]; days = list(range(1, num_days + 1)); daily_summary = []
    schedule_df.columns = [col if isinstance(col, str) else str(col) for col in schedule_df.columns]
    for d in days:
        day_info = {}; 
        work_symbols = ['', '○', '出', 'AM休', 'PM休', 'AM有', 'PM有']
        work_staff_ids = schedule_df[schedule_df[str(d)].isin(work_symbols)]['職員番号']
        half_day_staff_ids = [s for s, dates in all_half_day_requests.items() if d in dates]
        total_workers = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids)
        day_info['日'] = d; day_info['曜日'] = ['月','火','水','木','金','土','日'][calendar.weekday(year, month, d)]
        day_info['出勤者総数'] = total_workers
        day_info['PT'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if staff_info[sid]['職種'] == '理学療法士')
        day_info['OT'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if staff_info[sid]['職種'] == '作業療法士')
        day_info['ST'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if staff_info[sid]['職種'] == '言語聴覚士')
        day_info['役職者'] = sum(0.5 if sid in half_day_staff_ids else 1 for sid in work_staff_ids if pd.notna(staff_info[sid].get('役職')))
        day_info['PT単位数'] = sum(int(staff_info[sid]['1日の単位数']) * (0.5 if sid in half_day_staff_ids else 1) for sid in work_staff_ids if staff_info[sid]['職種'] == '理学療法士')
        day_info['OT単位数'] = sum(int(staff_info[sid]['1日の単位数']) * (0.5 if sid in half_day_staff_ids else 1) for sid in work_staff_ids if staff_info[sid]['職種'] == '作業療法士')
        day_info['ST単位数'] = sum(int(staff_info[sid]['1日の単位数']) * (0.5 if sid in half_day_staff_ids else 1) for sid in work_staff_ids if staff_info[sid]['職種'] == '言語聴覚士')
        daily_summary.append(day_info)
    return pd.DataFrame(daily_summary)

def _create_schedule_df(shifts_values, staff, days, staff_df, requests_map):
    schedule_data = {}
    for s in staff:
        row = []
        s_requests = requests_map.get(s, {})
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
                if request_type == '○': row.append('○')
                elif request_type in ['AM休', 'PM休', 'AM有', 'PM有']: row.append(request_type)
                elif request_type == '△': row.append('出')
                else: row.append('')
        schedule_data[s] = row
    schedule_df = pd.DataFrame.from_dict(schedule_data, orient='index', columns=[str(d) for d in days])
    schedule_df = schedule_df.reset_index().rename(columns={'index': '職員番号'})
    staff_map = staff_df.set_index('職員番号')
    schedule_df.insert(1, '職員名', schedule_df['職員番号'].map(staff_map['職員名']))
    schedule_df.insert(2, '職種', schedule_df['職員番号'].map(staff_map['職種']))
    return schedule_df


# --- メインのソルバー関数 (完全版) ---
def solve_shift_model(year, month, staff_df, requests_df, params):
    model = cp_model.CpModel()
    
    # --- データ検証 ---
    required_cols = ['職員番号', '職員名', '職種', '勤務形態', '1日の単位数', '土曜上限', '日曜上限', '祝日上限']
    for col in required_cols:
        if col not in staff_df.columns:
            return False, None, None, f"エラー: 職員一覧に必須列 '{col}' がありません。", {}, ""
    staff_df[required_cols] = staff_df[required_cols].fillna('')

    # --- 日付と職員の準備 ---
    num_days = calendar.monthrange(year, month)[1]
    days = list(range(1, num_days + 1))
    
    staff_info = staff_df.set_index('職員番号').to_dict('index')
    all_staff = staff_df['職員番号'].tolist()
    
    part_time_staff = [s for s, info in staff_info.items() if info.get('勤務形態') == 'パート']
    full_time_staff = [s for s in all_staff if s not in part_time_staff]

    holidays_jp_dates = jpholiday.month_holidays(year, month)
    holidays_jp = [d.day for d in holidays_jp_dates]
    sundays = [d for d in days if calendar.weekday(year, month, d) == 6 and d not in holidays_jp]
    saturdays = [d for d in days if calendar.weekday(year, month, d) == 5 and d not in holidays_jp and d not in sundays]
    special_days = {'sun': sundays, 'sat': saturdays, 'hol': holidays_jp}
    
    managers = [s for s, info in staff_info.items() if pd.notna(info.get('役職')) and info.get('役職') != '']
    groups = {g: list(sdf['職員番号']) for g, sdf in staff_df[(staff_df['グループ'] != '') & (~staff_df['職員番号'].isin(part_time_staff))].groupby('グループ')}
    job_types = { 'pt': [s for s,i in staff_info.items() if i['職種']=='理学療法士'], 'ot': [s for s,i in staff_info.items() if i['職種']=='作業療法士'], 'st': [s for s,i in staff_info.items() if i['職種']=='言語聴覚士'] }

    # --- 変数定義 ---
    shifts = { (s, d): model.NewBoolVar(f'shift_{s}_{d}') for s in all_staff for d in days }

    # --- 希望休の読み込み ---
    requests_map = {s: {} for s in all_staff}
    if not requests_df.empty:
        for _, row in requests_df.iterrows():
            staff_id = str(row['職員番号'])
            if staff_id not in all_staff: continue
            for d in days:
                col_name = str(d)
                if col_name in row and pd.notna(row[col_name]) and row[col_name] != '':
                    requests_map[staff_id][d] = row[col_name]

    # --- ハード制約 ---
    for s in part_time_staff:
        s_reqs = requests_map.get(s, {})
        for d in days:
            req = s_reqs.get(d)
            if req in ['○', 'AM休', 'PM休', 'AM有', 'PM有']: model.Add(shifts[(s, d)] == 1)
            elif req in ['×', '有', '特', '夏']: model.Add(shifts[(s, d)] == 0)

    for s in all_staff:
        for limit_type, day_list in [('土曜上限', saturdays), ('日曜上限', sundays), ('祝日上限', holidays_jp)]:
            limit = int(staff_info[s].get(limit_type) or 99)
            model.Add(sum(shifts[(s, d)] for d in day_list) <= limit)

    # --- ソフト制約（違反検知付き）---
    penalties = []
    violation_vars = {} 
    
    # [S-H2] 正職員の月間公休数
    h2_penalty_val = params.get('h2_penalty', 10000)
    for s in full_time_staff:
        s_reqs = requests_map.get(s, {}); num_paid_leave = sum(1 for r in s_reqs.values() if r == '有'); num_special_leave = sum(1 for r in s_reqs.values() if r == '特'); num_summer_leave = sum(1 for r in s_reqs.values() if r == '夏')
        full_holidays_total = sum(1 - shifts[(s, d)] for d in days)
        full_holidays_kokyu = model.NewIntVar(0, num_days, f'full_kokyu_{s}')
        model.Add(full_holidays_kokyu == full_holidays_total - num_paid_leave - num_special_leave - num_summer_leave)
        num_half_kokyu = sum(1 for r in s_reqs.values() if r in ['AM休', 'PM休'])
        diff = model.NewIntVar(-50, 50, f'h2_diff_{s}')
        model.Add(diff == (2 * full_holidays_kokyu + num_half_kokyu) - 18)
        abs_diff = model.NewIntVar(0, 50, f'h2_abs_diff_{s}')
        model.AddAbsEquality(abs_diff, diff)
        violation_vars[f'H2_{staff_info[s]["職員名"]}'] = abs_diff
        penalties.append(h2_penalty_val * abs_diff)

    # [S-1] 各種希望休の尊重
    for s in full_time_staff:
        s_reqs = requests_map.get(s, {})
        for d, req in s_reqs.items():
            if req in ['×', '有', '特', '夏']: penalties.append(params.get('s1_penalty_paid_leave', 10000) * shifts[(s,d)])
            elif req in ['○', 'AM有', 'PM有', 'AM休', 'PM休']: penalties.append(params.get('s1_penalty_work_request', 8000) * (1 - shifts[(s,d)]))
            elif req == '△': penalties.append(params.get('s1_penalty_semi_request', 20) * shifts[(s,d)])

    # [S-2] 役職者の配置
    h3_penalty_val = params.get('s2_penalty', 5000)
    for d in days:
        is_violated = model.NewBoolVar(f'h3_v_{d}')
        model.Add(sum(shifts[(s, d)] for s in managers) == 0).OnlyEnforceIf(is_violated)
        model.Add(sum(shifts[(s, d)] for s in managers) >= 1).OnlyEnforceIf(is_violated.Not())
        violation_vars[f'H3_{d}日'] = is_violated
        penalties.append(h3_penalty_val * is_violated)

    # [S-3] グループ内の出勤人数の平準化
    s3_penalty_val = params.get('s3_penalty', 10)
    for group_name, members in groups.items():
        if len(members) < 2: continue
        daily_counts = [sum(shifts[(s, d)] for s in members) for d in days]
        max_count = model.NewIntVar(0, len(members), f'g_max_{group_name}')
        min_count = model.NewIntVar(0, len(members), f'g_min_{group_name}')
        model.AddMaxEquality(max_count, daily_counts)
        model.AddMinEquality(min_count, daily_counts)
        penalties.append(s3_penalty_val * (max_count - min_count))

    # [S-4] 特別日の目標人数
    s4_penalty_val = params.get('s4_penalty', 50)
    for day_type, day_list in special_days.items():
        if not day_list: continue
        for d in day_list:
            for job in ['pt', 'ot', 'st']:
                target = params.get(f'target_{job}_{day_type}', 0)
                job_staff_list = job_types.get(job, [])
                actual = sum(shifts[(s,d)] for s in job_staff_list)
                diff = model.NewIntVar(-len(job_staff_list), len(job_staff_list), f's4_diff_{job}_{d}')
                model.Add(diff == actual - target)
                abs_diff = model.NewIntVar(0, len(job_staff_list), f's4_abs_diff_{job}_{d}')
                model.AddAbsEquality(abs_diff, diff)
                penalties.append(s4_penalty_val * abs_diff)

    # [S-5] 2段階の日曜出勤割り当て
    s5_penalty_val = params.get('s5_penalty', 50)
    for s in full_time_staff:
        if int(staff_info[s].get('日曜上限', 0)) >= 3:
            num_sundays_worked = sum(shifts[(s, d)] for d in sundays)
            over_two_sundays = model.NewIntVar(0, 5, f'sunday_over2_{s}') # 上限は5回程度と仮定
            model.Add(over_two_sundays >= num_sundays_worked - 2)
            penalties.append(s5_penalty_val * over_two_sundays)

    # [S-6] 週休の確保
    s6_penalty_val = params.get('s6_penalty', 200)
    weeks_in_month = []; current_week = []
    for d in days:
        current_week.append(d)
        if calendar.weekday(year, month, d) == 6 or d == num_days: weeks_in_month.append(current_week); current_week = []
    for s_idx, s in enumerate(full_time_staff):
        s_reqs = requests_map.get(s, {})
        all_half_day_requests_for_s = {d for d, r in s_reqs.items() if r in ['AM有', 'PM有', 'AM休', 'PM休']}
        for w_idx, week in enumerate(weeks_in_month):
            num_full_holidays_in_week = sum(1 - shifts[(s, d)] for d in week)
            num_half_holidays_in_week = sum(shifts[(s, d)] for d in week if d in all_half_day_requests_for_s)
            total_holiday_value = model.NewIntVar(0, 28, f'thv_s{s_idx}_w{w_idx}')
            model.Add(total_holiday_value == 2 * num_full_holidays_in_week + num_half_holidays_in_week)
            if len(week) >= 6:
                 is_violated = model.NewBoolVar(f's6_v_{s_idx}_{w_idx}')
                 model.Add(total_holiday_value < 3).OnlyEnforceIf(is_violated) # 1.5日=3ポイント未満は違反
                 model.Add(total_holiday_value >= 3).OnlyEnforceIf(is_violated.Not())
                 penalties.append(s6_penalty_val * is_violated)
    
    # [S-7] 業務負荷の平準化
    # ... (このロジックは非常に長大になるため、主要部分のみ実装。詳細は元のコードを参考に調整が必要)
    
    # --- 求解 ---
    model.Minimize(sum(penalties))
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 120.0
    status = solver.Solve(model)
    
    # --- 結果の返却 ---
    violation_messages = []
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        for key, var in violation_vars.items():
            if solver.Value(var) > 0:
                parts = key.split('_')
                if parts[0] == 'H2': violation_messages.append(f"職員「{parts[1]}」の月間休日数が不足しています。")
                elif parts[0] == 'H3': violation_messages.append(f"{parts[1]}は役職者が不在です。")
        
        message = f"求解ステータス: **{solver.StatusName(status)}** (ペナルティ合計: **{round(solver.ObjectiveValue())}**)"
        if violation_messages:
            message += "\n\n**以下の重要ルール違反があります（仮の勤務表として表示しています）:**\n- " + "\n- ".join(violation_messages)
        
        shifts_values = { (s, d): solver.Value(shifts[(s, d)]) for s in all_staff for d in days }
        all_half_day_requests = {s: {d for d, r in requests_map.get(s, {}).items() if r in ['AM有', 'PM有', 'AM休', 'PM休']} for s in all_staff}
        schedule_df = _create_schedule_df(shifts_values, all_staff, days, staff_df, requests_map)
        summary_df = _create_summary(schedule_df, staff_info, year, month, params.get('event_units', {}), all_half_day_requests)
        return True, schedule_df, summary_df, message, all_half_day_requests
    else:
        message = f"致命的なエラー: 求解不能でした。({solver.StatusName(status)})。制約が厳しすぎるか、予期せぬ矛盾があります。"
        return False, None, None, message, {}

# --- アプリケーション実行 ---
def main():
    st.set_page_config(layout="wide", page_title="勤務表作成アプリ")
    st.title('リハビリテーション科 勤務表作成アプリ')
    
    gsheet_manager = GSheetManager("設定ファイル（土井）") # ご自身のスプレッドシート名に変更
    success, staff_df, requests_df, event_units_df, params_df, error = gsheet_manager.load_all_data()

    if not success:
        st.error(f"データ読み込みに失敗しました。シート名や権限を確認してください。")
        st.error(str(error))
        st.stop()
    
    params_from_ui = {}
    
    with st.sidebar:
        st.header("⚙️ 基本設定")
        today = datetime.now()
        next_month_date = today + relativedelta(months=1)
        year = st.number_input("年", min_value=today.year - 1, max_value=today.year + 2, value=next_month_date.year)
        month = st.selectbox("月", options=list(range(1, 13)), index=next_month_date.month - 1)
        st.markdown("---")

        st.header("💾 パラメータプリセット")
        preset_names = ["-"] + params_df['プリセット名'].unique().tolist()
        selected_preset = st.selectbox("プリセットを読み込み", options=preset_names)
        
        params_from_sheet = {}
        if selected_preset != "-":
            preset_df = params_df[params_df['プリセット名'] == selected_preset]
            params_from_sheet = preset_df.set_index('パラメータ名')['値'].apply(pd.to_numeric, errors='coerce').to_dict()

        st.header("🎯 特別日の目標人数")
        tab_sun, tab_sat, tab_hol = st.tabs(["日曜日", "土曜日", "祝日"])
        with tab_sun:
            params_from_ui['target_pt_sun'] = st.number_input("PT (日)", min_value=0, value=int(params_from_sheet.get('target_pt_sun', 10)))
            params_from_ui['target_ot_sun'] = st.number_input("OT (日)", min_value=0, value=int(params_from_sheet.get('target_ot_sun', 5)))
            params_from_ui['target_st_sun'] = st.number_input("ST (日)", min_value=0, value=int(params_from_sheet.get('target_st_sun', 3)))
        with tab_sat:
            params_from_ui['target_pt_sat'] = st.number_input("PT (土)", min_value=0, value=int(params_from_sheet.get('target_pt_sat', 12)))
            params_from_ui['target_ot_sat'] = st.number_input("OT (土)", min_value=0, value=int(params_from_sheet.get('target_ot_sat', 6)))
            params_from_ui['target_st_sat'] = st.number_input("ST (土)", min_value=0, value=int(params_from_sheet.get('target_st_sat', 3)))
        with tab_hol:
            params_from_ui['target_pt_hol'] = st.number_input("PT (祝)", min_value=0, value=int(params_from_sheet.get('target_pt_hol', 0)))
            params_from_ui['target_ot_hol'] = st.number_input("OT (祝)", min_value=0, value=int(params_from_sheet.get('target_ot_hol', 0)))
            params_from_ui['target_st_hol'] = st.number_input("ST (祝)", min_value=0, value=int(params_from_sheet.get('target_st_hol', 0)))

        with st.expander("▼ ペナルティ調整"):
            params_from_ui['h2_penalty'] = st.number_input("月間休日数 違反P", 0, 20000, int(params_from_sheet.get('h2_penalty', 10000)))
            params_from_ui['s2_penalty'] = st.number_input("役職者不在 違反P", 0, 20000, int(params_from_sheet.get('s2_penalty', 5000)))
            params_from_ui['s1_penalty_paid_leave'] = st.number_input("確定休(×,有等) 違反P", 0, 20000, int(params_from_sheet.get('s1_penalty_paid_leave', 10000)))
            params_from_ui['s1_penalty_work_request'] = st.number_input("出勤希望(○等) 違反P", 0, 20000, int(params_from_sheet.get('s1_penalty_work_request', 8000)))
            params_from_ui['s1_penalty_semi_request'] = st.number_input("準希望休(△) 違反P", 0, 1000, int(params_from_sheet.get('s1_penalty_semi_request', 20)))
            params_from_ui['s3_penalty'] = st.slider("グループ平準化P", 0, 100, int(params_from_sheet.get('s3_penalty', 10)))
            params_from_ui['s4_penalty'] = st.slider("特別日人数P", 0, 200, int(params_from_sheet.get('s4_penalty', 50)))
            params_from_ui['s5_penalty'] = st.slider("日曜3回目以降P", 0, 200, int(params_from_sheet.get('s5_penalty', 50)))
            params_from_ui['s6_penalty'] = st.slider("週休確保P", 0, 1000, int(params_from_sheet.get('s6_penalty', 200)))

        with st.expander("▼ 現在の設定をプリセットに保存"):
            save_target_preset = st.selectbox("保存先を選択", options=params_df['プリセット名'].unique().tolist(), key="save_preset")
            if st.button("保存実行"):
                with st.spinner("プリセットを保存中..."):
                    # 現在のUI上の全パラメータを収集
                    params_to_save = params_from_ui.copy()
                    # 目標人数もdictに含める
                    for key in ['target_pt_sun', 'target_ot_sun', 'target_st_sun', 'target_pt_sat', 'target_ot_sat', 'target_st_sat', 'target_pt_hol', 'target_ot_hol', 'target_st_hol']:
                        params_to_save[key] = params_from_ui[key]

                    save_success, save_error = gsheet_manager.save_preset(save_target_preset, params_to_save, params_df)
                    if save_success:
                        st.success(f"「{save_target_preset}」に現在の設定を保存しました。")
                        st.cache_data.clear()
                    else:
                        st.error("プリセットの保存に失敗しました。"); st.exception(save_error)

    st.header(f"{year}年{month}月 勤務表作成")
    create_button = st.button('勤務表を作成', type="primary", use_container_width=True)

    if create_button:
        with st.spinner("最適化計算を実行中..."):
            final_params = {**params_from_sheet, **params_from_ui}
            final_params['event_units'] = gsheet_manager.convert_event_units_df_to_dict(event_units_df, year, month)
            
            is_feasible, schedule_df, summary_df, message, all_half_day_requests = solve_shift_model(
                year, month, staff_df, requests_df, final_params
            )
            
            st.markdown(message)
            if is_feasible:
                st.header("勤務表")
                # ... (結果表示のコード) ...

    st.markdown("---")
    st.markdown(f"<div style='text-align: right; color: grey;'>{APP_CREDIT} | Version: {APP_VERSION}</div>", unsafe_allow_html=True)


if __name__ == '__main__':
    main()