# streamlit_step_app_advanced.py
import streamlit as st
import pandas as pd
import json
import io
from datetime import datetime, date, time
import pytz # For timezone conversion
import xlsxwriter # For Excel formatting
import plotly.express as px
import plotly.graph_objects as go
# from statsmodels.tsa.seasonal import seasonal_decompose # Optional

# --- App Configuration and UI Text ---
APP_TITLE = "Advanced Step Analytics Dashboard 🚀"
APP_SUBHEADER = "Upload your JSON step data files, choose your timezone, and explore your activity!"
FILE_UPLOAD_LABEL = "Drop your step data here (JSON files only):"
NO_FILES_MSG = "Oops! You need to give me at least one step note to make a report."
PROCESSING_MSG = "Abracadabra! Making your step report... 🧙‍♂️"
SUCCESS_MSG = "Ta-da! Your step report is ready! Click the button below to download. 📄⬇️"
ERROR_MSG = "Uh oh! Something went a bit wiggly. Please check your files or try again. 😟"
NO_STEP_DATA_MSG = "Hmm, I couldn't find any 'STEPS' information in your files. Make sure they have step data!"


# --- Helper Functions ---
def get_common_timezones():
    common_tz = [
        'UTC', 'US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific',
        'Europe/London', 'Europe/Berlin', 'Europe/Paris',
        'Asia/Tokyo', 'Asia/Dubai', 'Asia/Kolkata',
        'Australia/Sydney', 'America/Sao_Paulo'
    ]
    valid_common_tz = [tz for tz in common_tz if tz in pytz.all_timezones_set]
    # Ensure 'America/New_York' is present if valid, otherwise ensure 'UTC' is an option
    final_tz_list = list(set(valid_common_tz + ['America/New_York']))
    if 'America/New_York' not in pytz.all_timezones_set and 'America/New_York' in final_tz_list:
        final_tz_list.remove('America/New_York')
    if 'UTC' not in final_tz_list and 'UTC' in pytz.all_timezones_set: # Ensure UTC is always an option
        final_tz_list.append('UTC')
    return sorted(list(set(final_tz_list)))


@st.cache_data # Caches the output of this function
def process_all_uploaded_files(uploaded_files, target_timezone_str):
    all_step_records = []
    target_tz = pytz.timezone(target_timezone_str)
    safe_target_tz_suffix = target_timezone_str.replace('/', '_').replace('-', '_')

    # Define dynamic local column names
    local_measured_dt_col = f'measuredDateTime_Local_{safe_target_tz_suffix}'
    local_date_obj_col = f'date_Local_obj_{safe_target_tz_suffix}'
    local_time_obj_col = f'time_Local_obj_{safe_target_tz_suffix}'
    local_hour_col = f'hour_Local_{safe_target_tz_suffix}'
    local_day_of_week_col = f'day_of_week_Local_{safe_target_tz_suffix}'
    local_date_dt_col = f'date_Local_dt_{safe_target_tz_suffix}'

    if not uploaded_files:
        return None, None, None, None, None

    for uploaded_file_idx, uploaded_file in enumerate(uploaded_files):
        try:
            # Ensure file pointer is at the beginning forgetvalue()
            uploaded_file.seek(0)
            file_content = uploaded_file.getvalue().decode("utf-8")
            content = json.loads(file_content)

            file_device_id = content.get("deviceId", f"N/A_file{uploaded_file_idx}")
            file_platform = content.get("platform", f"N/A_file{uploaded_file_idx}")

            if 'data' in content and 'STEPS' in content['data'] and isinstance(content['data']['STEPS'], list):
                step_entries = content['data']['STEPS']
                for entry_idx, entry in enumerate(step_entries):
                    record = {
                        'value': entry.get('value'),
                        'sourceName': entry.get('sourceName', 'Unknown Source'),
                        'measuredDateTime_UTC_str': entry.get('measuredDateTime'),
                        'deviceId': file_device_id,
                        'platform': file_platform,
                        'original_file': uploaded_file.name,
                        'original_entry_idx': entry_idx
                    }
                    all_step_records.append(record)
        except Exception as e:
            st.error(f"Error processing '{uploaded_file.name}': {e}")
            continue

    if not all_step_records:
        return None, None, None, None, None

    df_steps = pd.DataFrame(all_step_records)

    if df_steps.empty:
        return None, None, None, None, None

    df_steps['steps'] = pd.to_numeric(df_steps['value'], errors='coerce').fillna(0).astype(int)
    df_steps.dropna(subset=['measuredDateTime_UTC_str'], inplace=True)
    # Parse as UTC. If string has offset, it's converted. If naive, it's assumed UTC.
    df_steps['measuredDateTime_UTC'] = pd.to_datetime(df_steps['measuredDateTime_UTC_str'], errors='coerce', utc=True)
    df_steps.dropna(subset=['measuredDateTime_UTC'], inplace=True) # Drop if parsing failed

    # Local timezone columns
    df_steps[local_measured_dt_col] = df_steps['measuredDateTime_UTC'].dt.tz_convert(target_tz)
    df_steps[local_date_obj_col] = df_steps[local_measured_dt_col].dt.date # Python date object
    df_steps[local_time_obj_col] = df_steps[local_measured_dt_col].dt.time
    df_steps[local_hour_col] = df_steps[local_measured_dt_col].dt.hour
    df_steps[local_day_of_week_col] = df_steps[local_measured_dt_col].dt.day_name()
    # Convert local date object to naive datetime (midnight) for grouping & Plotly
    df_steps[local_date_dt_col] = pd.to_datetime(df_steps[local_date_obj_col])

    # UTC columns
    df_steps['date_UTC_obj'] = df_steps['measuredDateTime_UTC'].dt.date # Python date object
    df_steps['hour_UTC'] = df_steps['measuredDateTime_UTC'].dt.hour
    # Convert UTC date object to naive datetime (midnight) for grouping & Plotly
    df_steps['date_UTC_dt'] = pd.to_datetime(df_steps['date_UTC_obj'])

    df_steps.sort_values(by=local_measured_dt_col, inplace=True)

    # --- DEBUG: Uncomment to inspect date conversions ---
    # if not df_steps.empty:
    #     st.subheader("DEBUG: Sample df_steps after date processing")
    #     debug_cols = ['measuredDateTime_UTC', local_measured_dt_col, 'date_UTC_dt', local_date_dt_col, 'steps']
    #     # Ensure all debug_cols exist before trying to display them
    #     display_debug_cols = [col for col in debug_cols if col in df_steps.columns]
    #     if display_debug_cols:
    #         st.dataframe(df_steps[display_debug_cols].head(10))
    #     else:
    #         st.write("Debug columns for date processing not available.")
    # --- END DEBUG ---

    raw_df_excel = df_steps[[
        'measuredDateTime_UTC', local_measured_dt_col,
        'steps', 'sourceName', 'deviceId', 'platform', 'original_file'
    ]].copy()
    raw_df_excel.rename(columns={
        'measuredDateTime_UTC': 'Timestamp (UTC)',
        local_measured_dt_col: f'Timestamp ({target_timezone_str})',
        'sourceName': 'Source App', 'deviceId': 'Device ID',
        'original_file': 'Original Filename', 'steps': 'Steps'
    }, inplace=True)

    # --- Local Time Aggregations ---
    daily_local_steps_total = df_steps.groupby(local_date_dt_col)['steps'].sum().reset_index()
    daily_local_steps_total.rename(columns={'steps': 'Total Steps', local_date_dt_col: 'Date'}, inplace=True)

    if daily_local_steps_total.empty: # If no data even after processing
        # Prepare empty dataframes with expected columns for graceful handling downstream
        empty_daily_cols = ['Date', 'Total Steps', 'Min Steps (single entry)', 'Max Steps (single entry)', 'Avg Steps (per entry)', 'Peak Hour', 'Avg Steps per Active Hour', '7-Day Rolling Avg Steps']
        daily_summary_df = pd.DataFrame(columns=empty_daily_cols)
        empty_utc_cols = ['Date (UTC)', 'Total Steps (UTC)']
        utc_daily_df = pd.DataFrame(columns=empty_utc_cols)
        key_insights_df = pd.DataFrame(columns=['Date (Local)', 'Insight/Anomaly Type', 'Description'])
        return df_steps, raw_df_excel, daily_summary_df, utc_daily_df, key_insights_df

    daily_min_max_avg_local = df_steps.groupby(local_date_dt_col)['steps'].agg(
        min_entry='min', max_entry='max', avg_entry='mean'
    ).reset_index()
    daily_min_max_avg_local.rename(columns={local_date_dt_col: 'Date', 'min_entry': 'Min Steps (single entry)', 'max_entry': 'Max Steps (single entry)', 'avg_entry': 'Avg Steps (per entry)'}, inplace=True)
    daily_min_max_avg_local['Avg Steps (per entry)'] = daily_min_max_avg_local['Avg Steps (per entry)'].round(1)

    hourly_sums_local = df_steps.groupby([local_date_dt_col, local_hour_col])['steps'].sum().reset_index()
    if not hourly_sums_local.empty:
        idx_local = hourly_sums_local.groupby([local_date_dt_col])['steps'].idxmax()
        daily_peak_hour_local = hourly_sums_local.loc[idx_local, [local_date_dt_col, local_hour_col]]
        daily_peak_hour_local.rename(columns={local_hour_col: 'Peak Hour', local_date_dt_col: 'Date'}, inplace=True)
    else:
        daily_peak_hour_local = pd.DataFrame(columns=['Date', 'Peak Hour'])
        daily_peak_hour_local['Date'] = pd.to_datetime(daily_peak_hour_local['Date'])


    avg_hourly_steps_local = hourly_sums_local.groupby(local_date_dt_col)['steps'].mean().round(1).reset_index()
    avg_hourly_steps_local.rename(columns={'steps': 'Avg Steps per Active Hour', local_date_dt_col: 'Date'}, inplace=True)
    
    rolling_df_local = daily_local_steps_total.copy()
    rolling_df_local['Date'] = pd.to_datetime(rolling_df_local['Date']) # Ensure Date is datetime for set_index
    rolling_df_local = rolling_df_local.set_index('Date').sort_index()
    if 'Total Steps' in rolling_df_local.columns and not rolling_df_local.empty:
        rolling_df_local['7-Day Rolling Avg Steps'] = rolling_df_local['Total Steps'].rolling(window=7, min_periods=1).mean().round(1)
    else:
        rolling_df_local['7-Day Rolling Avg Steps'] = pd.NA

    daily_summary_df = pd.merge(daily_local_steps_total, daily_min_max_avg_local, on='Date', how='left')
    if not daily_peak_hour_local.empty:
         daily_summary_df = pd.merge(daily_summary_df, daily_peak_hour_local, on='Date', how='left')
    else: daily_summary_df['Peak Hour'] = pd.NA # Use pandas NA for missing numeric/object
    if not avg_hourly_steps_local.empty:
        daily_summary_df = pd.merge(daily_summary_df, avg_hourly_steps_local, on='Date', how='left')
    else: daily_summary_df['Avg Steps per Active Hour'] = pd.NA
    
    daily_summary_df['Date'] = pd.to_datetime(daily_summary_df['Date']) # Ensure Date is datetime type
    daily_summary_df = pd.merge(daily_summary_df, rolling_df_local[['7-Day Rolling Avg Steps']].reset_index(), on='Date', how='left')

    # --- UTC Aggregations ---
    utc_daily_df = df_steps.groupby('date_UTC_dt')['steps'].sum().reset_index()
    utc_daily_df.rename(columns={'steps': 'Total Steps (UTC)', 'date_UTC_dt': 'Date (UTC)'}, inplace=True)
    utc_daily_df['Date (UTC)'] = pd.to_datetime(utc_daily_df['Date (UTC)']) # Ensure datetime type

    # --- DEBUG: Uncomment to inspect aggregated DFs ---
    # if daily_summary_df is not None and not daily_summary_df.empty:
    #     st.subheader(f"DEBUG: daily_summary_df ({target_timezone_str})")
    #     st.dataframe(daily_summary_df.head())
    # if utc_daily_df is not None and not utc_daily_df.empty:
    #     st.subheader("DEBUG: utc_daily_df")
    #     st.dataframe(utc_daily_df.head())
    # --- END DEBUG ---

    key_insights_df = pd.DataFrame(columns=['Date (Local)', 'Insight/Anomaly Type', 'Description'])
    if 'Total Steps' in daily_summary_df.columns and not daily_summary_df.empty:
        avg_all_days = daily_summary_df['Total Steps'].mean()
        std_all_days = daily_summary_df['Total Steps'].std()
        if pd.notna(avg_all_days) and pd.notna(std_all_days) and std_all_days > 0:
            threshold_factor = 1.5
            very_active = daily_summary_df[daily_summary_df['Total Steps'] > avg_all_days + threshold_factor * std_all_days]
            for _, row in very_active.iterrows():
                key_insights_df.loc[len(key_insights_df)] = [row['Date'].strftime('%Y-%m-%d'), 'High Activity', f"Steps: {row['Total Steps']:.0f}"]
            very_inactive = daily_summary_df[daily_summary_df['Total Steps'] < avg_all_days - threshold_factor * std_all_days]
            for _, row in very_inactive.iterrows():
                 key_insights_df.loc[len(key_insights_df)] = [row['Date'].strftime('%Y-%m-%d'), 'Low Activity', f"Steps: {row['Total Steps']:.0f}"]
        if 'Peak Hour' in daily_summary_df.columns:
            late_peak = daily_summary_df[daily_summary_df['Peak Hour'].notna() & (daily_summary_df['Peak Hour'] >= 22)]
            for _, row in late_peak.iterrows():
                 key_insights_df.loc[len(key_insights_df)] = [row['Date'].strftime('%Y-%m-%d'), 'Late Peak Activity', f"Peak hour: {int(row['Peak Hour']):02d}:00"]
            early_peak = daily_summary_df[daily_summary_df['Peak Hour'].notna() & (daily_summary_df['Peak Hour'] <= 6)]
            for _, row in early_peak.iterrows():
                 key_insights_df.loc[len(key_insights_df)] = [row['Date'].strftime('%Y-%m-%d'), 'Early Peak Activity', f"Peak hour: {int(row['Peak Hour']):02d}:00"]
    
    # Return DFs with datetime objects for dates for UI; Excel function will handle string conversion
    return df_steps, raw_df_excel, daily_summary_df, utc_daily_df, key_insights_df


def make_datetime_columns_naive(df_input):
    if df_input is None: return None
    df = df_input.copy()
    for col in df.select_dtypes(include=['datetimetz']):
        df[col] = df[col].dt.tz_localize(None)
    return df

def create_excel_report_enhanced(raw_df, daily_local_df_orig, daily_utc_df_orig, insights_df, local_tz_name):
    # Create copies for Excel formatting to not alter UI dataframes
    daily_local_df_excel = daily_local_df_orig.copy()
    if 'Date' in daily_local_df_excel.columns:
        daily_local_df_excel['Date'] = pd.to_datetime(daily_local_df_excel['Date']).dt.strftime('%Y-%m-%d')

    daily_utc_df_excel = daily_utc_df_orig.copy()
    if 'Date (UTC)' in daily_utc_df_excel.columns:
        daily_utc_df_excel['Date (UTC)'] = pd.to_datetime(daily_utc_df_excel['Date (UTC)']).dt.strftime('%Y-%m-%d')
    
    insights_df_excel = insights_df.copy() # Dates are already strings in insights_df

    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine='xlsxwriter',
                        datetime_format='yyyy-mm-dd hh:mm:ss',
                        date_format='yyyy-mm-dd') as writer:
        workbook = writer.book
        header_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#D9E1F2', 'text_wrap': True})
        date_str_fmt = workbook.add_format({'align': 'left', 'num_format': '@'}) # Treat as text explicitly for string dates
        datetime_excel_fmt = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss', 'align': 'left'})
        center_fmt = workbook.add_format({'align': 'center'})
        num_fmt = workbook.add_format({'num_format': '#,##0', 'align': 'right'})
        float_fmt = workbook.add_format({'num_format': '#,##0.0', 'align': 'right'})

        raw_df_excel_naive = make_datetime_columns_naive(raw_df)
        
        short_local_tz_name = local_tz_name.split('/')[-1].replace("_", " ")
        raw_sheet_name = f'Raw Data ({short_local_tz_name}, UTC)'[:31] # Ensure sheet name length
        daily_local_sheet_name = f'Daily Summary ({short_local_tz_name})'[:31]

        sheets_data = {
            raw_sheet_name: raw_df_excel_naive,
            daily_local_sheet_name: daily_local_df_excel,
            'Daily Summary (UTC)': daily_utc_df_excel,
            'Key Insights & Anomalies': insights_df_excel
        }

        for sheet_name_key, df_sheet in sheets_data.items():
            actual_sheet_name = sheet_name_key
            if df_sheet is None or df_sheet.empty:
                worksheet = workbook.add_worksheet(actual_sheet_name)
                worksheet.write('A1', "No data available for this sheet.", center_fmt)
                worksheet.set_column('A:A', 30)
                continue

            df_sheet.to_excel(writer, sheet_name=actual_sheet_name, index=False, startrow=1, header=False)
            worksheet = writer.sheets[actual_sheet_name]

            for col_num, value in enumerate(df_sheet.columns.values):
                worksheet.write(0, col_num, str(value), header_format)

            for col_num, col_name in enumerate(df_sheet.columns):
                max_len = max(len(str(col_name)), df_sheet[col_name].astype(str).map(len).max(skipna=True) if not df_sheet[col_name].empty else 0)
                col_width = min(max(max_len, 12) + 2, 45)
                current_format = None
                col_dtype_series = df_sheet[col_name]

                if df_sheet is raw_df_excel_naive and pd.api.types.is_datetime64_any_dtype(col_dtype_series.dtype) and not pd.api.types.is_datetime64tz_dtype(col_dtype_series.dtype):
                    current_format = datetime_excel_fmt
                    col_width = 20
                elif col_name in ['Date', 'Date (UTC)', 'Date (Local)'] and (df_sheet is daily_local_df_excel or df_sheet is daily_utc_df_excel or df_sheet is insights_df_excel):
                     current_format = date_str_fmt
                     col_width = 12
                elif pd.api.types.is_integer_dtype(col_dtype_series.dtype) or col_name in ['Peak Hour', 'Steps', 'Total Steps', 'Min Steps (single entry)', 'Max Steps (single entry)', 'Total Steps (UTC)']:
                    current_format = num_fmt
                elif pd.api.types.is_float_dtype(col_dtype_series.dtype) or col_name in ['Avg Steps (per entry)', 'Avg Steps per Active Hour', '7-Day Rolling Avg Steps']:
                    current_format = float_fmt
                
                worksheet.set_column(col_num, col_num, col_width, current_format)
            
            if not df_sheet.empty:
                worksheet.autofilter(0, 0, df_sheet.shape[0], df_sheet.shape[1] - 1)
        
        if daily_local_df_excel is not None and not daily_local_df_excel.empty and 'Total Steps' in daily_local_df_excel.columns and 'Date' in daily_local_df_excel.columns:
            ws_daily_local = writer.sheets[daily_local_sheet_name]
            max_row_chart = daily_local_df_excel.shape[0] + 1
            if max_row_chart > 1 :
                chart_daily_local = workbook.add_chart({'type': 'column'})
                date_col_idx = daily_local_df_excel.columns.get_loc('Date')
                steps_col_idx = daily_local_df_excel.columns.get_loc('Total Steps')
                date_col_letter = xlsxwriter.utility.xl_col_to_name(date_col_idx)
                steps_col_letter = xlsxwriter.utility.xl_col_to_name(steps_col_idx)

                chart_daily_local.add_series({
                    'name':       f"='{daily_local_sheet_name}'!${steps_col_letter}$1",
                    'categories': f"='{daily_local_sheet_name}'!${date_col_letter}$2:${date_col_letter}${max_row_chart}",
                    'values':     f"='{daily_local_sheet_name}'!${steps_col_letter}$2:${steps_col_letter}${max_row_chart}",
                    'fill':       {'color': '#4F81BD'}, 'border': {'color': '#3E6494'}
                })
                if '7-Day Rolling Avg Steps' in daily_local_df_excel.columns:
                    roll_avg_col_idx = daily_local_df_excel.columns.get_loc('7-Day Rolling Avg Steps')
                    roll_avg_col_letter = xlsxwriter.utility.xl_col_to_name(roll_avg_col_idx)
                    chart_daily_local.add_series({
                        'name':       f"='{daily_local_sheet_name}'!${roll_avg_col_letter}$1",
                        'categories': f"='{daily_local_sheet_name}'!${date_col_letter}$2:${date_col_letter}${max_row_chart}",
                        'values':     f"='{daily_local_sheet_name}'!${roll_avg_col_letter}$2:${roll_avg_col_letter}${max_row_chart}",
                        'type': 'line', 'line': {'color': '#C0504D', 'width': 1.5, 'dash_type': 'dash'}, 'y2_axis': True
                    })
                    chart_daily_local.set_y2_axis({'name': 'Rolling Avg', 'major_gridlines': {'visible': False}, 'num_format': '#,##0.0'})
                
                chart_daily_local.set_title({'name': f'Daily Steps ({short_local_tz_name}) & 7-Day Rolling Avg'})
                chart_daily_local.set_x_axis({'name': 'Date', 'date_axis': True, 'label_position': 'low', 'num_font': {'rotation': -45}})
                chart_daily_local.set_y_axis({'name': 'Total Steps', 'major_gridlines': {'visible': True}, 'num_format': '#,##0'})
                chart_daily_local.set_legend({'position': 'top'})
                chart_daily_local.set_size({'width': 720, 'height': 380})
                chart_insert_col = xlsxwriter.utility.xl_col_to_name(daily_local_df_excel.shape[1] + 1)
                ws_daily_local.insert_chart(f'{chart_insert_col}2', chart_daily_local)

                median_steps = pd.to_numeric(daily_local_df_excel['Total Steps'], errors='coerce').median()
                if pd.notna(median_steps):
                    fmt_below = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
                    fmt_above = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
                    ws_daily_local.conditional_format(f'{steps_col_letter}2:{steps_col_letter}{max_row_chart}',
                                                        {'type': 'cell', 'criteria': '<', 'value': median_steps, 'format': fmt_below})
                    ws_daily_local.conditional_format(f'{steps_col_letter}2:{steps_col_letter}{max_row_chart}',
                                                        {'type': 'cell', 'criteria': '>=', 'value': median_steps, 'format': fmt_above})
    return output_buffer.getvalue()


# --- Streamlit App UI ---
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.markdown(APP_SUBHEADER)

st.sidebar.header("⚙️ Report Settings")
available_timezones = get_common_timezones()
# Ensure a sensible default index if 'America/New_York' or 'UTC' is not found (highly unlikely for UTC)
default_tz = 'America/New_York' if 'America/New_York' in available_timezones else ('UTC' if 'UTC' in available_timezones else (available_timezones[0] if available_timezones else None))
default_idx = available_timezones.index(default_tz) if default_tz else 0

selected_timezone = st.sidebar.selectbox(
    "Choose your local timezone for analysis:",
    options=available_timezones,
    index=default_idx
)

uploaded_files = st.sidebar.file_uploader(
    FILE_UPLOAD_LABEL,
    type=["json"],
    accept_multiple_files=True,
    key="file_uploader"
)

if uploaded_files:
    st.info(f"⏳ Processing {len(uploaded_files)} file(s) for timezone: {selected_timezone}...")
    
    valid_files_for_processing = []
    for up_file_check in uploaded_files:
        try:
            up_file_check.seek(0)
            temp_stringio = io.StringIO(up_file_check.getvalue().decode("utf-8"))
            temp_content = json.load(temp_stringio)
            if 'data' in temp_content and 'STEPS' in temp_content['data'] and \
               isinstance(temp_content['data']['STEPS'], list) and \
               len(temp_content['data']['STEPS']) > 0:
                valid_files_for_processing.append(up_file_check)
                up_file_check.seek(0) 
            else:
                st.warning(f"File '{up_file_check.name}' seems empty or doesn't have 'STEPS' data. Skipped.")
        except Exception as pe:
            st.warning(f"Could not pre-check file '{up_file_check.name}': {pe}. Skipped.")
            up_file_check.seek(0)

    if not valid_files_for_processing:
        if uploaded_files: 
            st.error(NO_STEP_DATA_MSG + " None of the uploaded files were valid after pre-check.")
        # Ensure these are defined for the 'else' block of the main 'if uploaded_files:'
        df_steps_processed, raw_df_for_excel, daily_summary_df_for_ui, utc_daily_df_for_ui, key_insights_df = None, None, None, None, None
    else:
        df_steps_processed, raw_df_for_excel, daily_summary_df_for_ui, utc_daily_df_for_ui, key_insights_df = \
            process_all_uploaded_files(valid_files_for_processing, selected_timezone)

    if df_steps_processed is not None and not df_steps_processed.empty:
        st.success("✅ Data processed successfully!")

        # Dynamic column names based on selected timezone for UI access
        safe_selected_tz_suffix = selected_timezone.replace('/', '_').replace('-', '_')
        ui_local_measured_dt_col = f'measuredDateTime_Local_{safe_selected_tz_suffix}'
        ui_local_date_obj_col = f'date_Local_obj_{safe_selected_tz_suffix}'
        ui_local_hour_col = f'hour_Local_{safe_selected_tz_suffix}'
        ui_local_day_of_week_col = f'day_of_week_Local_{safe_selected_tz_suffix}'

        tab_overview, tab_daily_local, tab_daily_utc, tab_hourly, tab_sources, tab_insights = st.tabs([
            "📊 Overview", f"📅 Daily ({selected_timezone.split('/')[-1]})", "🌍 Daily (UTC)",
            "🕒 Hourly Patterns", "📱 Step Sources", "💡 Key Insights"
        ])
        
        with tab_overview:
            st.subheader("Overall Activity Snapshot")
            if daily_summary_df_for_ui is not None and not daily_summary_df_for_ui.empty and 'Total Steps' in daily_summary_df_for_ui.columns:
                total_days_data = daily_summary_df_for_ui['Date'].nunique()
                overall_avg_daily_steps = daily_summary_df_for_ui['Total Steps'].mean()
                most_active_day_val = None
                if daily_summary_df_for_ui['Total Steps'].notna().any():
                     most_active_day_val = daily_summary_df_for_ui.loc[daily_summary_df_for_ui['Total Steps'].idxmax()]

                col1, col2, col3 = st.columns(3)
                col1.metric("Total Days with Data", f"{total_days_data}")
                if pd.notna(overall_avg_daily_steps):
                    col2.metric(f"Avg. Daily Steps ({selected_timezone.split('/')[-1]})", f"{overall_avg_daily_steps:,.0f}")
                if most_active_day_val is not None and isinstance(most_active_day_val, pd.Series) and 'Date' in most_active_day_val and 'Total Steps' in most_active_day_val:
                    col3.metric(f"Busiest Day ({selected_timezone.split('/')[-1]})", 
                                f"{most_active_day_val['Date'].strftime('%b %d, %Y')} ({most_active_day_val['Total Steps']:,.0f} steps)")
                
                if not df_steps_processed.empty and ui_local_day_of_week_col in df_steps_processed.columns and ui_local_date_obj_col in df_steps_processed.columns and ui_local_measured_dt_col in df_steps_processed:
                    df_steps_processed[f'{ui_local_day_of_week_col}_num'] = df_steps_processed[ui_local_measured_dt_col].dt.dayofweek
                    
                    avg_steps_by_dow_series = df_steps_processed.groupby(f'{ui_local_day_of_week_col}_num')['steps'].sum().div(
                                              df_steps_processed.groupby(f'{ui_local_day_of_week_col}_num')[ui_local_date_obj_col].nunique()
                                            ).round(0)
                    if not avg_steps_by_dow_series.empty:
                        day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
                        current_day_names = [day_names[i] for i in avg_steps_by_dow_series.index if i < len(day_names)]
                        if len(current_day_names) == len(avg_steps_by_dow_series): # Check if mapping was successful
                            avg_steps_by_dow_series.index = current_day_names
                            st.write(f"**Busiest Day of Week (Avg):** {avg_steps_by_dow_series.idxmax()} ({avg_steps_by_dow_series.max():,.0f} steps)")
                    
                    if ui_local_hour_col in df_steps_processed.columns:
                        avg_steps_by_hod_series = df_steps_processed.groupby(ui_local_hour_col)['steps'].sum().div(
                                                  df_steps_processed.groupby(ui_local_hour_col)[ui_local_date_obj_col].nunique()
                                                ).round(0)
                        if not avg_steps_by_hod_series.empty:
                            busiest_hour = avg_steps_by_hod_series.idxmax()
                            st.write(f"**Busiest Hour of Day (Avg):** {busiest_hour:02d}:00 - { (busiest_hour + 1) % 24:02d}:00 ({avg_steps_by_hod_series.max():,.0f} steps)")
            else: st.info("No summary data for overview. Upload files or check data format.")

        with tab_daily_local:
            st.subheader(f"Daily Step Analysis ({selected_timezone.split('/')[-1]})")
            if daily_summary_df_for_ui is not None and not daily_summary_df_for_ui.empty:
                fig_daily_local = px.bar(daily_summary_df_for_ui, x='Date', y='Total Steps', title=f'Daily Total Steps ({selected_timezone.split("/")[-1]})')
                if '7-Day Rolling Avg Steps' in daily_summary_df_for_ui.columns and daily_summary_df_for_ui['7-Day Rolling Avg Steps'].notna().any():
                    fig_daily_local.add_trace(go.Scatter(x=daily_summary_df_for_ui['Date'], y=daily_summary_df_for_ui['7-Day Rolling Avg Steps'],
                                                         mode='lines', name='7-Day Rolling Avg',
                                                         line=dict(color='rgba(255,127,80,0.8)', width=2, dash='dash')))
                fig_daily_local.update_layout(xaxis_title='Date', yaxis_title='Number of Steps')
                st.plotly_chart(fig_daily_local, use_container_width=True)
                st.dataframe(daily_summary_df_for_ui.set_index('Date').style.format({"Total Steps": "{:,.0f}", "Min Steps (single entry)": "{:,.0f}", "Max Steps (single entry)": "{:,.0f}", "Avg Steps (per entry)": "{:,.1f}", "Avg Steps per Active Hour": "{:,.1f}", "7-Day Rolling Avg Steps": "{:,.1f}"}), use_container_width=True)

                st.markdown("---")
                st.subheader("View All Entries for a Specific Local Day")
                min_date_local = daily_summary_df_for_ui['Date'].min().date() if not daily_summary_df_for_ui['Date'].empty else date.today()
                max_date_local = daily_summary_df_for_ui['Date'].max().date() if not daily_summary_df_for_ui['Date'].empty else date.today()
                
                selected_local_day_view = st.date_input("Select a local date to view details:", 
                                                        value=min_date_local, 
                                                        min_value=min_date_local, 
                                                        max_value=max_date_local,
                                                        key="local_date_detail_selector")
                if selected_local_day_view and ui_local_date_obj_col in df_steps_processed.columns:
                    day_details_local_df = df_steps_processed[df_steps_processed[ui_local_date_obj_col] == selected_local_day_view]
                    if not day_details_local_df.empty:
                        display_cols_local = [ui_local_measured_dt_col, 'measuredDateTime_UTC', 'steps', 'sourceName', 'original_file']
                        st.dataframe(day_details_local_df[display_cols_local].sort_values(by=ui_local_measured_dt_col), use_container_width=True)
                    else:
                        st.info(f"No step entries found for local date: {selected_local_day_view.strftime('%Y-%m-%d')}")

            else: st.info("No daily summary data for local timezone. Upload files or check data.")

        with tab_daily_utc:
            st.subheader("Daily Step Analysis (UTC)")
            if utc_daily_df_for_ui is not None and not utc_daily_df_for_ui.empty:
                fig_daily_utc = px.bar(utc_daily_df_for_ui, x='Date (UTC)', y='Total Steps (UTC)', title='Daily Total Steps (UTC)')
                fig_daily_utc.update_layout(xaxis_title='Date (UTC)', yaxis_title='Number of Steps')
                st.plotly_chart(fig_daily_utc, use_container_width=True)
                st.dataframe(utc_daily_df_for_ui.set_index('Date (UTC)').style.format({"Total Steps (UTC)": "{:,.0f}"}), use_container_width=True)

                st.markdown("---")
                st.subheader("View All Entries for a Specific UTC Day")
                min_date_utc = utc_daily_df_for_ui['Date (UTC)'].min().date() if not utc_daily_df_for_ui['Date (UTC)'].empty else date.today()
                max_date_utc = utc_daily_df_for_ui['Date (UTC)'].max().date() if not utc_daily_df_for_ui['Date (UTC)'].empty else date.today()

                selected_utc_day_view = st.date_input("Select a UTC date to view details:", 
                                                      value=min_date_utc, 
                                                      min_value=min_date_utc, 
                                                      max_value=max_date_utc,
                                                      key="utc_date_detail_selector")
                if selected_utc_day_view and 'date_UTC_obj' in df_steps_processed.columns:
                    day_details_utc_df = df_steps_processed[df_steps_processed['date_UTC_obj'] == selected_utc_day_view]
                    if not day_details_utc_df.empty:
                        display_cols_utc = ['measuredDateTime_UTC', ui_local_measured_dt_col, 'steps', 'sourceName', 'original_file']
                        # Ensure ui_local_measured_dt_col exists before trying to use it for display
                        if ui_local_measured_dt_col not in day_details_utc_df.columns:
                            display_cols_utc.remove(ui_local_measured_dt_col)
                        st.dataframe(day_details_utc_df[display_cols_utc].sort_values(by='measuredDateTime_UTC'), use_container_width=True)
                    else:
                        st.info(f"No step entries found for UTC date: {selected_utc_day_view.strftime('%Y-%m-%d')}")
            else: st.info("No daily summary data for UTC. Upload files or check data.")

        with tab_hourly:
            st.subheader(f"Hourly Activity Patterns ({selected_timezone.split('/')[-1]})")
            if not df_steps_processed.empty and ui_local_hour_col in df_steps_processed.columns and ui_local_day_of_week_col in df_steps_processed.columns:
                hourly_activity = df_steps_processed.pivot_table(values='steps', index=ui_local_hour_col, columns=ui_local_day_of_week_col, aggfunc='mean').fillna(0)
                days_ordered = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                ordered_cols = [day for day in days_ordered if day in hourly_activity.columns]
                if ordered_cols: hourly_activity = hourly_activity[ordered_cols]

                if not hourly_activity.empty:
                    fig_heatmap = px.imshow(hourly_activity, labels=dict(x="Day of Week", y="Hour of Day", color="Average Steps"),
                                            color_continuous_scale="Viridis", aspect="auto",
                                            title=f"Average Steps by Hour and Day of Week ({selected_timezone.split('/')[-1]})")
                    fig_heatmap.update_xaxes(side="bottom")
                    st.plotly_chart(fig_heatmap, use_container_width=True)
                else: st.info("Not enough data for hourly heatmap.")

                avg_steps_per_hour_overall = df_steps_processed.groupby(ui_local_hour_col)['steps'].mean().round(0).reset_index()
                if not avg_steps_per_hour_overall.empty:
                    fig_hod_avg = px.bar(avg_steps_per_hour_overall, x=ui_local_hour_col, y='steps',
                                         title=f'Overall Average Steps per Hour ({selected_timezone.split("/")[-1]})',
                                         labels={ui_local_hour_col: 'Hour of Day (0-23)', 'steps': 'Average Steps'})
                    st.plotly_chart(fig_hod_avg, use_container_width=True)
                else: st.info("Not enough data for overall hourly average.")
            else: st.info("No data for hourly patterns. Check data or required columns.")

        with tab_sources:
            st.subheader("Step Source Analysis")
            if not df_steps_processed.empty and 'sourceName' in df_steps_processed.columns:
                source_counts = df_steps_processed.groupby('sourceName')['steps'].sum().sort_values(ascending=False).reset_index()
                if not source_counts.empty:
                    fig_source_pie = px.pie(source_counts, values='steps', names='sourceName', title='Proportion of Steps by Source Application', hole=0.3)
                    st.plotly_chart(fig_source_pie, use_container_width=True)
                    fig_source_bar = px.bar(source_counts, x='sourceName', y='steps', title='Total Steps by Source Application', labels={'sourceName': 'Source App', 'steps': 'Total Steps'})
                    st.plotly_chart(fig_source_bar, use_container_width=True)
                else: st.info("No data on step sources.")
            else: st.info("No data for source analysis. Check 'sourceName' column.")

        with tab_insights:
            st.subheader("Key Insights & Detected Anomalies")
            if key_insights_df is not None and not key_insights_df.empty:
                st.dataframe(key_insights_df.set_index('Date (Local)'), use_container_width=True)
            else: st.info("No specific insights or anomalies detected with current data/rules.")

        st.sidebar.markdown("---")
        st.sidebar.header("📥 Download Report")
        excel_data_bytes = create_excel_report_enhanced(raw_df_for_excel, daily_summary_df_for_ui, utc_daily_df_for_ui, key_insights_df, selected_timezone)
        
        # Use pytz to make datetime object timezone-aware for filename
        try:
            now_in_selected_tz = datetime.now(pytz.timezone(selected_timezone))
        except pytz.UnknownTimeZoneError: # Fallback to UTC if selected_timezone is bad
            now_in_selected_tz = datetime.now(pytz.utc)

        report_timestamp = now_in_selected_tz.strftime("%Y%m%d_%H%M%S")
        excel_filename = f"Step_Analytics_Report_{report_timestamp}_{selected_timezone.replace('/', '_')}.xlsx"
        
        st.sidebar.download_button(
            label="Download Full Excel Report", data=excel_data_bytes,
            file_name=excel_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    elif uploaded_files and (df_steps_processed is None or df_steps_processed.empty):
        # This case means files were uploaded, pre-check might have passed some, but processing yielded nothing.
        # Error messages should have been shown by process_all_uploaded_files or pre-check.
        # If no specific error shown yet, provide a general one.
        if not any(st.session_state.get(msg_type) for msg_type in ['error', 'warning']): # Check if error/warning already shown by Streamlit's internal mechanism
            st.error(ERROR_MSG + " " + NO_STEP_DATA_MSG + " No processable step records found in the valid files.")
else:
    st.info("☝️ Upload your JSON files and select a timezone using the sidebar to begin!")

st.markdown("---")
st.caption("Advanced Step Analytics App | Your Personal Data Explorer")