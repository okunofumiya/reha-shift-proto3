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
APP_VERSION = "proto.2.2.3" # ファイルチェック機能強化版
APP_CREDIT = "Okuno with 🤖 Gemini and Claude"

# --- ヘルパー関数: サマリー作成 ---
def _create_summary(schedule_df, staff_info_dict, year, month, event_units, all_half_day_requests):
    num_days = calendar.monthrange(year, month)[1]; days = list(range(1, num_days + 1)); daily_summary = []
    schedule_df.columns = [col if isinstance(col, str) else int(col) for col in schedule_df.columns]
    for d in days:
        day_info = {}; 
        work_symbols = ['', '○', '出', 'AM休', 'PM休', 'AM有', 'PM有']
        work_staff_ids = schedule_df[schedule_df[d].isin(work_symbols)]['職員番号']
        half_day_staff_ids = [s for s, dates in all_half_day_requests.items() if d in dates]
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
            pt_units = sum(int(staff_info_dict[sid]['1日の単位数']) * (0.5 if sid in half_day_staff_ids else 1) for sid in work_staff_ids if staff_info_dict[sid]['職種'] == '理学療法士')
            ot_units = sum(int(staff_info_dict[sid]['1日の単位数']) * (0.5 if sid in half_day_staff_ids else 1) for sid in work_staff_ids if staff_info_dict[sid]['職種'] == '作業療法士')
            st_units = sum(int(staff_info_dict[sid]['1日の単位数']) * (0.5 if sid in half_day_staff_ids else 1) for sid in work_staff_ids if staff_info_dict[sid]['職種'] == '言語聴覚士')
            day_info['PT単位数'] = pt_units; day_info['OT単位数'] = ot_units; day_info['ST単位数'] = st_units
            day_info['PT+OT単位数'] = pt_units + ot_units
            total_event_unit = event_units['all'].get(d, 0) + event_units['pt'].get(d, 0) + event_units['ot'].get(d, 0) + event_units['st'].get(d, 0)
            day_info['特別業務単位数'] = total_event_unit
        else:
            day_info['PT単位数'] = '-'; day_info['OT単位数'] = '-'; day_info['ST単位数'] = '-';
            day_info['PT+OT単位数'] = '-'; day_info['特別業務単位数'] = '-'
        daily_summary.append(day_info)
    return pd.DataFrame(daily_summary)

def _create_schedule_df(shifts_values, staff, days, staff_df, requests_map):
    schedule_data = {}
    for s in staff:
        row = []
        s_requests = requests_map.get(s, {})
        for d in days:
            request_type = s_requests.get(d)
            if shifts_values.get((s, d), 0) == 0: # 休みの場合
                if request_type == '×': row.append('×')
                elif request_type == '△': row.append('△')
                elif request_type == '有': row.append('有')
                elif request_type == '特': row.append('特')
                elif request_type == '夏': row.append('夏')
                else: row.append('-')
            else: # 出勤の場合
                if request_type == '○': row.append('○')
                elif request_type in ['AM休', 'PM休', 'AM有', 'PM有']: row.append(request_type)
                elif request_type == '△': row.append('出')
                else: row.append('')
        schedule_data[s] = row
    schedule_df = pd.DataFrame.from_dict(schedule_data, orient='index', columns=days)
    schedule_df = schedule_df.reset_index().rename(columns={'index': '職員番号'})
    staff_map = staff_df.set_index('職員番号')
    schedule_df.insert(1, '職員名', schedule_df['職員番号'].map(staff_map['職員名']))
    schedule_df.insert(2, '職種', schedule_df['職員番号'].map(staff_map['職種']))
    return schedule_df

# --- メインのソルバー関数 ---
def solve_shift_model(params):
    year, month = params['year'], params['month']
    num_days = calendar.monthrange(year, month)[1]; days = list(range(1, num_days + 1))
    
    ### 変更点 1: 日曜上限の必須チェック ###
    if '日曜上限' not in params['staff_df'].columns:
        st.error("エラー: 職員一覧に必須列 '日曜上限' がありません。")
        return False, pd.DataFrame(), pd.DataFrame(), "エラー: 職員一覧に必須列 '日曜上限' がありません。", None
    if params['staff_df']['日曜上限'].isnull().any():
        st.error("エラー: 職員一覧の '日曜上限' に空欄があります。全員分入力してください。")
        return False, pd.DataFrame(), pd.DataFrame(), "エラー: 職員一覧の '日曜上限' に空欄があります。", None

    staff = params['staff_df']['職員番号'].tolist()
    staff_info = params['staff_df'].set_index('職員番号').to_dict('index')
    params['staff_info'] = staff_info 
    params['staff'] = staff 
    sundays = [d for d in days if calendar.weekday(year, month, d) == 6]; weekdays = [d for d in days if d not in sundays]
    params['sundays'] = sundays; params['weekdays'] = weekdays; params['days'] = days 
    
    managers = [s for s in staff if pd.notna(staff_info[s]['役職'])]; pt_staff = [s for s in staff if staff_info[s]['職種'] == '理学療法士']
    ot_staff = [s for s in staff if staff_info[s]['職種'] == '作業療法士']; st_staff = [s for s in staff if staff_info[s]['職種'] == '言語聴覚士']
    params['pt_staff'] = pt_staff; params['ot_staff'] = ot_staff; params['st_staff'] = st_staff 
    
    kaifukuki_staff = [s for s in staff if staff_info[s].get('役割1') == '回復期専従']; kaifukuki_pt = [s for s in kaifukuki_staff if staff_info[s]['職種'] == '理学療法士']
    kaifukuki_ot = [s for s in kaifukuki_staff if staff_info[s]['職種'] == '作業療法士']; gairai_staff = [s for s in staff if staff_info[s].get('役割1') == '外来PT']
    chiiki_staff = [s for s in staff if staff_info[s].get('役割1') == '地域包括専従']
    # sunday_off_staffは使わなくなるので削除
    params['kaifukuki_pt'] = kaifukuki_pt; params['kaifukuki_ot'] = kaifukuki_ot; params['gairai_staff'] = gairai_staff 
    job_types = {'PT': pt_staff, 'OT': ot_staff, 'ST': st_staff}
    params['job_types'] = job_types 
    
    requests_map = {s: {} for s in staff}
    request_types = ['×', '△', '○', '有', '特', '夏', 'AM有', 'PM有', 'AM休', 'PM休']
    for index, row in params['requests_df'].iterrows():
        staff_id = row['職員番号']
        if staff_id not in staff: continue
        # YYYY-MM-DD形式の日付文字列を作成して希望休を読み込む場合
        # from datetime import date
        # for d in days:
        #     col_name = date(year, month, d).strftime('%Y-%m-%d')
        #     if col_name in row and pd.notna(row[col_name]):
        #         requests_map[staff_id][d] = row[col_name]
        #
        # 現在の「1,2,3...」の列名で読み込む場合
        for d in days:
             col_name = str(d)
             if col_name in row and pd.notna(row[col_name]):
                 requests_map[staff_id][d] = row[col_name]

    params['requests_map'] = requests_map

    model = cp_model.CpModel(); shifts = {}
    for s in staff:
        for d in days: shifts[(s, d)] = model.NewBoolVar(f'shift_{s}_{d}')

    if params['h1_on']:
        # (h1のコードは変更なし)
        for s in staff:
            s_reqs = requests_map.get(s, {})
            num_paid_leave = sum(1 for r in s_reqs.values() if r == '有')
            num_special_leave = sum(1 for r in s_reqs.values() if r == '特')
            num_summer_leave = sum(1 for r in s_reqs.values() if r == '夏')
            num_half_kokyu = sum(1 for r in s_reqs.values() if r in ['AM休', 'PM休'])
            full_holidays_total = sum(1 - shifts[(s, d)] for d in days)
            full_holidays_kokyu = model.NewIntVar(0, num_days, f'full_kokyu_{s}')
            model.Add(full_holidays_kokyu == full_holidays_total - num_paid_leave - num_special_leave - num_summer_leave)
            model.Add(2 * full_holidays_kokyu + num_half_kokyu == 18)

    if params['h2_on']:
        # (h2のコードは変更なし)
        for s, reqs in requests_map.items():
            for d, req_type in reqs.items():
                if req_type in ['×', '有', '特', '夏']: model.Add(shifts[(s, d)] == 0)
                elif req_type in ['○', 'AM有', 'PM有', 'AM休', 'PM休']: model.Add(shifts[(s, d)] == 1)

    if params['h3_on']:
        # (h3のコードは変更なし)
        for d in days: model.Add(sum(shifts[(s, d)] for s in managers) >= 1)
    
    ### 変更点 2: 古いH4とH5のルールを無効化 ###
    # H4はUIから削除しても良いが、ここではオフの場合の動作を記述
    if params.get('h4_on', False):
        st.warning("H4は廃止されました。職員一覧の「日曜上限」に0を入力してください。")
        # 何もしない

    # H5もUIから削除しても良い
    if params.get('h5_on', False):
        st.warning("H5は廃止されました。「日曜上限」ルールに統合されています。")
        # 何もしない

    ### 変更点 3: 新しい日曜上限のハード制約 ###
    for s in staff:
        # 各職員の日曜出勤回数が、職員情報にある「日曜上限」を超えないようにする
        sunday_limit = int(staff_info[s]['日曜上限'])
        model.Add(sum(shifts[(s, d)] for d in sundays) <= sunday_limit)

    penalties = []
    
    ### 変更点 4: 2段階割り当てのための新しいソフト制約 ###
    # このペナルティの値は、他のペナルティより十分大きいが、必須ではない程度の値に設定
    sunday_overwork_penalty = 50 
    for s in staff:
        # 日曜上限が3以上の職員に対してのみ、ペナルティを考慮する
        if int(staff_info[s]['日曜上限']) >= 3:
            num_sundays_worked = sum(shifts[(s, d)] for d in sundays)
            # 2回を超えた出勤回数を計算するための変数
            over_two_sundays = model.NewIntVar(0, 5, f'sunday_over2_{s}')
            # (実働日曜数 - 2) が 0 より大きい場合、その差分がover_two_sundaysになる
            # 例: 実働3回なら over_two_sundays = 1
            model.Add(over_two_sundays >= num_sundays_worked - 2)
            model.Add(over_two_sundays >= 0)
            
            # 2回を超えた出勤回数に対してペナルティを課す
            penalties.append(sunday_overwork_penalty * over_two_sundays)
    
    if params['s4_on']:
        for s, reqs in requests_map.items():
            for d, req_type in reqs.items():
                if req_type == '△':
                    penalties.append(params['s4_penalty'] * shifts[(s, d)])

    if params['s0_on'] or params['s2_on']:
        weeks_in_month = []; current_week = []
        for d in days:
            current_week.append(d)
            if calendar.weekday(year, month, d) == 5 or d == num_days: weeks_in_month.append(current_week); current_week = []
        params['weeks_in_month'] = weeks_in_month
        
        for s_idx, s in enumerate(staff):
            s_reqs = requests_map.get(s, {})
            all_full_requests = {d for d, r in s_reqs.items() if r in ['×', '有', '特', '夏', '△']}
            all_half_day_requests = {d for d, r in s_reqs.items() if r in ['AM有', 'PM有', 'AM休', 'PM休']}

            for w_idx, week in enumerate(weeks_in_month):
                if sum(1 for d in week if d in all_full_requests) >= 3: continue
                
                num_full_holidays_in_week = sum(1 - shifts[(s, d)] for d in week)
                
                num_half_holidays_in_week = sum(shifts[(s, d)] for d in week if d in all_half_day_requests)

                total_holiday_value = model.NewIntVar(0, 28, f'thv_s{s_idx}_w{w_idx}')
                model.Add(total_holiday_value == 2 * num_full_holidays_in_week + num_half_holidays_in_week)

                if len(week) == 7 and params['s0_on']:
                    violation = model.NewBoolVar(f'f_w_v_s{s_idx}_w{w_idx}'); model.Add(total_holiday_value < 3).OnlyEnforceIf(violation); model.Add(total_holiday_value >= 3).OnlyEnforceIf(violation.Not()); penalties.append(params['s0_penalty'] * violation)
                elif len(week) < 7 and params['s2_on']:
                    violation = model.NewBoolVar(f'p_w_v_s{s_idx}_w{w_idx}'); model.Add(total_holiday_value < 1).OnlyEnforceIf(violation); model.Add(total_holiday_value >= 1).OnlyEnforceIf(violation.Not()); penalties.append(params['s2_penalty'] * violation)
    
    if any([params['s1a_on'], params['s1b_on'], params['s1c_on']]):
        for d in sundays:
            pt_on_sunday = sum(shifts[(s, d)] for s in pt_staff); ot_on_sunday = sum(shifts[(s, d)] for s in ot_staff); st_on_sunday = sum(shifts[(s, d)] for s in st_staff)
            if params['s1a_on']:
                total_pt_ot = pt_on_sunday + ot_on_sunday; total_diff = model.NewIntVar(-50, 50, f't_d_{d}'); model.Add(total_diff == total_pt_ot - (params['target_pt'] + params['target_ot'])); abs_total_diff = model.NewIntVar(0, 50, f'a_t_d_{d}'); model.AddAbsEquality(abs_total_diff, total_diff); penalties.append(params['s1a_penalty'] * abs_total_diff)
            if params['s1b_on']:
                pt_diff = model.NewIntVar(-30, 30, f'p_d_{d}'); model.Add(pt_diff == pt_on_sunday - params['target_pt']); pt_penalty = model.NewIntVar(0, 30, f'p_p_{d}'); model.Add(pt_penalty >= pt_diff - params['tolerance']); model.Add(pt_penalty >= -pt_diff - params['tolerance']); penalties.append(params['s1b_penalty'] * pt_penalty)
                ot_diff = model.NewIntVar(-30, 30, f'o_d_{d}'); model.Add(ot_diff == ot_on_sunday - params['target_ot']); ot_penalty = model.NewIntVar(0, 30, f'o_p_{d}'); model.Add(ot_penalty >= ot_diff - params['tolerance']); model.Add(ot_penalty >= -ot_diff - params['tolerance']); penalties.append(params['s1b_penalty'] * ot_penalty)
            if params['s1c_on']:
                st_diff = model.NewIntVar(-10, 10, f's_d_{d}'); model.Add(st_diff == st_on_sunday - params['target_st']); abs_st_diff = model.NewIntVar(0, 10, f'a_s_d_{d}'); model.AddAbsEquality(abs_st_diff, st_diff); penalties.append(params['s1c_penalty'] * abs_st_diff)
    if params['s3_on']:
        for d in days:
            num_gairai_off = sum(1 - shifts[(s, d)] for s in gairai_staff); penalty = model.NewIntVar(0, len(gairai_staff), f'g_p_{d}'); model.Add(penalty >= num_gairai_off - 1); penalties.append(params['s3_penalty'] * penalty)
    if params['s5_on']:
        for d in days:
            kaifukuki_pt_on = sum(shifts[(s, d)] for s in kaifukuki_pt)
            kaifukuki_ot_on = sum(shifts[(s, d)] for s in kaifukuki_ot)
            model.Add(kaifukuki_pt_on + kaifukuki_ot_on >= 1)
            pt_present = model.NewBoolVar(f'k_p_p_{d}'); ot_present = model.NewBoolVar(f'k_o_p_{d}'); model.Add(kaifukuki_pt_on >= 1).OnlyEnforceIf(pt_present); model.Add(kaifukuki_pt_on == 0).OnlyEnforceIf(pt_present.Not()); model.Add(kaifukuki_ot_on >= 1).OnlyEnforceIf(ot_present); model.Add(kaifukuki_ot_on == 0).OnlyEnforceIf(ot_present.Not()); penalties.append(params['s5_penalty'] * (1 - pt_present)); penalties.append(params['s5_penalty'] * (1 - ot_present))
    
    if params['s6_on']:
        unit_penalty_weight = params.get('s6_penalty_heavy', 4) if params.get('high_flat_penalty') else params.get('s6_penalty', 2)
        event_units = params['event_units']
        all_half_day_requests = {s: {d for d, r in reqs.items() if r in ['AM有', 'PM有', 'AM休', 'PM休']} for s, reqs in requests_map.items()}

        total_weekday_units_by_job = {}
        for job, members in job_types.items():
            if not members:
                total_weekday_units_by_job[job] = 0
                continue
            total_units = sum(int(staff_info[s]['1日の単位数']) * (len(weekdays) / num_days) * (num_days - 9 - sum(1 for r in requests_map.get(s, {}).values() if r in ['有','特','夏'])) for s in members)
            total_weekday_units_by_job[job] = total_units
        
        total_all_jobs_units = sum(total_weekday_units_by_job.values())
        ratios = {job: total_units / total_all_jobs_units if total_all_jobs_units > 0 else 0 for job, total_units in total_weekday_units_by_job.items()}
        
        avg_residual_units_by_job = {}
        total_event_units_all = sum(event_units['all'].values())
        
        for job, members in job_types.items():
            if not weekdays or not members:
                avg_residual_units_by_job[job] = 0
                continue
            total_event_units_job = sum(event_units[job.lower()].values())
            total_event_units_for_job = total_event_units_job + (total_event_units_all * ratios.get(job, 0))
            avg_residual_units_by_job[job] = (total_weekday_units_by_job.get(job, 0) - total_event_units_for_job) / len(weekdays)

        params['avg_residual_units_by_job'] = avg_residual_units_by_job
        params['ratios'] = ratios

        for job, members in job_types.items():
            if not members: continue
            avg_residual_units = avg_residual_units_by_job.get(job, 0)
            ratio = ratios.get(job, 0)
            
            for d in weekdays:
                provided_units_expr_list = []
                for s in members:
                    unit = int(staff_info[s]['1日の単位数'])
                    is_half = d in all_half_day_requests.get(s, set())
                    constant_unit = int(unit * 0.5) if is_half else unit

                    term = model.NewIntVar(0, constant_unit, f'p_u_s{s}_d{d}')
                    model.Add(term == shifts[(s,d)] * constant_unit)
                    provided_units_expr_list.append(term)
                
                provided_units_expr = sum(provided_units_expr_list)
                
                event_unit_for_day = event_units[job.lower()].get(d, 0) + (event_units['all'].get(d, 0) * ratio)
                
                residual_units_expr = model.NewIntVar(-4000, 4000, f'r_{job}_{d}')
                model.Add(residual_units_expr == provided_units_expr - round(event_unit_for_day))
                
                diff_expr = model.NewIntVar(-4000, 4000, f'u_d_{job}_{d}')
                model.Add(diff_expr == residual_units_expr - round(avg_residual_units))
                
                abs_diff_expr = model.NewIntVar(0, 4000, f'a_u_d_{job}_{d}')
                model.AddAbsEquality(abs_diff_expr, diff_expr)
                penalties.append(unit_penalty_weight * abs_diff_expr)

    model.Minimize(sum(penalties))
    solver = cp_model.CpSolver(); solver.parameters.max_time_in_seconds = 60.0; status = solver.Solve(model)
    
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        shifts_values = {(s, d): solver.Value(shifts[(s, d)]) for s in staff for d in days}
        all_half_day_requests = {s: {d for d, r in reqs.items() if r in ['AM有', 'PM有', 'AM休', 'PM休']} for s, reqs in requests_map.items()}
        schedule_df = _create_schedule_df(shifts_values, staff, days, params['staff_df'], requests_map)
        summary_df = _create_summary(schedule_df, staff_info, year, month, params['event_units'], all_half_day_requests)
        message = f"求解ステータス: **{solver.StatusName(status)}** (ペナルティ合計: **{round(solver.ObjectiveValue())}**)"
        
        return True, schedule_df, summary_df, message, all_half_day_requests
    else:
        message = f"致命的なエラー: ハード制約が矛盾しているため、勤務表を作成できませんでした。({solver.StatusName(status)})"
        return False, pd.DataFrame(), pd.DataFrame(), message, None

# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title('リハビリテーション科 勤務表作成アプリ')
today = datetime.now()
next_month_date = today + relativedelta(months=1)
default_year = next_month_date.year
default_month_index = next_month_date.month - 1
with st.expander("▼ 各種パラメータを設定する", expanded=True):
    c1, c2, c3 = st.columns(3)
    with c1:
        st.subheader("対象年月とファイル")
        year = st.number_input("年（西暦）", min_value=default_year - 5, max_value=default_year + 5, value=default_year)
        month = st.selectbox("月", options=list(range(1, 13)), index=default_month_index)
    with c2:
        st.subheader("日曜日の出勤人数設定")
        c2_1, c2_2, c2_3 = st.columns(3)
        with c2_1: target_pt = st.number_input("PT目標", min_value=0, value=10, step=1)
        with c2_2: target_ot = st.number_input("OT目標", min_value=0, value=5, step=1)
        with c2_3: target_st = st.number_input("ST目標", min_value=0, value=3, step=1)
    with c3:
        st.subheader("緩和条件と優先度")
        tolerance = st.number_input("PT/OT許容誤差(±)", min_value=0, max_value=5, value=1, help="PT/OTの合計人数が目標通りなら、それぞれの人数がこの値までずれてもペナルティを課しません。")
        tri_penalty_weight = st.slider("準希望休(△)の優先度", min_value=0, max_value=20, value=8, help="値が大きいほど△希望が尊重されます。")
    
    st.markdown("---")
    st.subheader(f"{year}年{month}月のイベント設定（各日の特別業務単位数を入力）")
    st.info("「全体」タブは職種を問わない業務、「PT/OT/ST」タブは各職種固有の業務を入力します。「全体」に入力された業務は、各職種の標準的な業務量比で自動的に按分されます。")
    
    event_tabs = st.tabs(["全体", "PT", "OT", "ST"])
    event_units_input = {'all': {}, 'pt': {}, 'ot': {}, 'st': {}}
    
    for i, tab_name in enumerate(['all', 'pt', 'ot', 'st']):
        with event_tabs[i]:
            day_counter = 1
            num_days_in_month = calendar.monthrange(year, month)[1]
            first_day_weekday = calendar.weekday(year, month, 1)
            
            cal_cols = st.columns(7)
            weekdays_jp = ['月', '火', '水', '木', '金', '土', '日']
            for day_idx, day_name in enumerate(weekdays_jp): cal_cols[day_idx].markdown(f"<p style='text-align: center;'><b>{day_name}</b></p>", unsafe_allow_html=True)
            
            for week_num in range(6):
                cols = st.columns(7)
                for day_of_week in range(7):
                    if (week_num == 0 and day_of_week < first_day_weekday) or day_counter > num_days_in_month:
                        cols[day_of_week].empty()
                        continue
                    with cols[day_of_week]:
                        is_sunday = calendar.weekday(year, month, day_counter) == 6
                        event_units_input[tab_name][day_counter] = st.number_input(
                            label=f"{day_counter}日", value=0, step=10, disabled=is_sunday, 
                            key=f"event_{tab_name}_{year}_{month}_{day_counter}"
                        )
                    day_counter += 1
                if day_counter > num_days_in_month: break

    st.markdown("---")
    create_button = st.button('勤務表を作成', type="primary", use_container_width=True)

with st.expander("▼ ルール検証モード（上級者向け）"):
    st.warning("注意: 各ルールのON/OFFやペナルティ値を変更することで、意図しない結果や、解が見つからない状況が発生する可能性があります。")
    st.markdown("---")
    st.subheader("ハード制約のON/OFF")
    h_cols = st.columns(5)
    params_ui = {}
    with h_cols[0]: params_ui['h1_on'] = st.toggle('H1: 月間休日数', value=True, key='h1')
    with h_cols[1]: params_ui['h2_on'] = st.toggle('H2: 希望休/有休', value=True, key='h2')
    with h_cols[2]: params_ui['h3_on'] = st.toggle('H3: 役職者配置', value=True, key='h3')
    with h_cols[3]: params_ui['h4_on'] = st.toggle('H4: 特定役割日曜休', value=True, key='h4')
    with h_cols[4]: params_ui['h5_on'] = st.toggle('H5: 日曜出勤上限', value=True, key='h5')
    st.markdown("---")
    st.subheader("ソフト制約のON/OFFとペナルティ設定")
    st.info("S0/S2の週休ルールは、半日休を0.5日分の休みとしてカウントし、完全な週は1.5日以上、不完全な週は0.5日以上の休日確保を目指します。")
    s_cols = st.columns(4)
    with s_cols[0]:
        params_ui['s0_on'] = st.toggle('S0: 完全週の週休1.5日', value=True, key='s0')
        params_ui['s0_penalty'] = st.number_input("S0 Penalty", value=200, disabled=not params_ui['s0_on'], key='s0p')
    with s_cols[1]:
        params_ui['s2_on'] = st.toggle('S2: 不完全週の週休0.5日', value=True, key='s2')
        params_ui['s2_penalty'] = st.number_input("S2 Penalty", value=25, disabled=not params_ui['s2_on'], key='s2p')
    with s_cols[2]:
        params_ui['s3_on'] = st.toggle('S3: 外来同時休', value=True, key='s3')
        params_ui['s3_penalty'] = st.number_input("S3 Penalty", value=10, disabled=not params_ui['s3_on'], key='s3p')
    with s_cols[3]:
        params_ui['s4_on'] = st.toggle('S4: 準希望休(△)尊重', value=True, key='s4')
        params_ui['s4_penalty'] = st.number_input("S4 Penalty", value=tri_penalty_weight, disabled=not params_ui['s4_on'], key='s4p')
    s_cols2 = st.columns(4)
    with s_cols2[0]:
        params_ui['s5_on'] = st.toggle('S5: 回復期配置', value=True, key='s5')
        params_ui['s5_penalty'] = st.number_input("S5 Penalty", value=5, disabled=not params_ui['s5_on'], key='s5p')
    with s_cols2[1]:
        params_ui['s6_on'] = st.toggle('S6: 職種別 業務負荷平準化', value=True, key='s6')
        c_s6_1, c_s6_2 = st.columns(2)
        params_ui['s6_penalty'] = c_s6_1.number_input("S6 標準P", value=2, disabled=not params_ui['s6_on'], key='s6p')
        params_ui['s6_penalty_heavy'] = c_s6_2.number_input("S6 強化P", value=4, disabled=not params_ui['s6_on'], key='s6ph')
    with s_cols2[2]:
        st.markdown("") 
    with s_cols2[3]:
        params_ui['high_flat_penalty'] = st.toggle('平準化ペナルティ強化', value=False, key='high_flat', help="S6のペナルティを「標準P」ではなく「強化P」で計算します。")
        
    st.markdown("##### S1: 日曜人数目標")
    s_cols3 = st.columns(3)
    with s_cols3[0]:
        params_ui['s1a_on'] = st.toggle('S1-a: PT/OT合計', value=True, key='s1a')
        params_ui['s1a_penalty'] = st.number_input("S1-a Penalty", value=50, disabled=not params_ui['s1a_on'], key='s1ap')
    with s_cols3[1]:
        params_ui['s1b_on'] = st.toggle('S1-b: PT/OT個別', value=True, key='s1b')
        params_ui['s1b_penalty'] = st.number_input("S1-b Penalty", value=40, disabled=not params_ui['s1b_on'], key='s1bp')
    with s_cols3[2]:
        params_ui['s1c_on'] = st.toggle('S1-c: ST目標', value=True, key='s1c')
        params_ui['s1c_penalty'] = st.number_input("S1-c Penalty", value=60, disabled=not params_ui['s1c_on'], key='s1cp')

if create_button:
    try:
        # StreamlitのSecretsから認証情報を取得してGoogleに接続
        creds_dict = st.secrets["gcp_service_account"]
        sa = gspread.service_account_from_dict(creds_dict)
        spreadsheet = sa.open("設定ファイル（土井）")
        
        # 職員一覧シートをDataFrameとして読み込み
        st.info("🔄 スプレッドシートから職員一覧を読み込んでいます...")
        staff_worksheet = spreadsheet.worksheet("職員一覧")
        staff_df = get_as_dataframe(staff_worksheet, dtype={'職員番号': str})
        staff_df.dropna(how='all', inplace=True) # 空白行を削除

        # 希望休一覧シートをDataFrameとして読み込み
        st.info("🔄 スプレッドシートから希望休一覧を読み込んでいます...")
        requests_worksheet = spreadsheet.worksheet("希望休一覧")
        requests_df = get_as_dataframe(requests_worksheet, dtype={'職員番号': str})
        requests_df.dropna(how='all', inplace=True) # 空白行を削除
        st.success("✅ データの読み込みが完了しました。")

        # --- ここから下は、元のコードと同じロジック ---
        params = {}
        params.update(params_ui)
        params['staff_df'] = staff_df
        params['requests_df'] = requests_df
        params['year'] = year; params['month'] = month
        params['target_pt'] = target_pt; params['target_ot'] = target_ot; params['target_st'] = target_st
        params['tolerance'] = tolerance; params['event_units'] = event_units_input
        
        # 必須列の存在チェック
        required_staff_cols = ['職員番号', '職種', '1日の単位数']
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
        
        # モデルを解く
        is_feasible, schedule_df, summary_df, message, all_half_day_requests = solve_shift_model(params)
        
        st.info(message)
        if is_feasible:
            # (この後の表示ロジックは元のコードのまま)
            st.header("勤務表")
            num_days = calendar.monthrange(year, month)[1]
            
            summary_T = summary_df.drop(columns=['日', '曜日']).T
            summary_T.columns = list(range(1, num_days + 1))
            summary_processed = summary_T.reset_index().rename(columns={'index': '職員名'})
            summary_processed['職員番号'] = summary_processed['職員名'].apply(lambda x: f"_{x}")
            summary_processed['職種'] = "サマリー"
            summary_processed = summary_processed[['職員番号', '職員名', '職種'] + list(range(1, num_days + 1))]
            
            final_df_for_display = pd.concat([schedule_df, summary_processed], ignore_index=True)
            days_header = list(range(1, num_days + 1))
            weekdays_header = [ ['月','火','水','木','金','土','日'][calendar.weekday(year, month, d)] for d in days_header]
            final_df_for_display.columns = pd.MultiIndex.from_tuples([('職員情報', '職員番号'), ('職員情報', '職員名'), ('職員情報', '職種')] + list(zip(days_header, weekdays_header)))
            
            def style_table(df):
                sunday_cols = [col for col in df.columns if col[1] == '日']
                styler = df.style.set_properties(**{'text-align': 'center'})
                for col in sunday_cols: styler = styler.set_properties(subset=[col], **{'background-color': '#fff0f0'})
                return styler
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                schedule_df.to_excel(writer, sheet_name='勤務表', index=False)
                summary_df.to_excel(writer, sheet_name='日別サマリー', index=False)
            excel_data = output.getvalue()
            st.download_button(label="📥 Excelでダウンロード", data=excel_data, file_name=f"schedule_{year}{month:02d}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.dataframe(style_table(final_df_for_display))
            
    except Exception as e:
        st.error(f'予期せぬエラーが発生しました: {e}')
        st.exception(e)

st.markdown("---")
st.markdown(f"<div style='text-align: right; color: grey;'>{APP_CREDIT} | Version: {APP_VERSION}</div>", unsafe_allow_html=True)