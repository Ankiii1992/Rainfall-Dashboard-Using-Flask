import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, render_template, request, jsonify
import json
from datetime import datetime, timedelta
import os

# ---------------------------- FLASK APP & GOOGLE SHEETS AUTH ----------------------------
app = Flask(__name__)

# Load Google Sheets client using the service account file
def get_gsheet_client():
    """Authenticates and returns a gspread client."""
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file('service_account.json', scopes=scope)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"Authentication failed: {e}")
        return None

# ---------------------------- RAINFALL CATEGORY LOGIC ----------------------------
color_map = {
    "No Rain": "#f8f8f8", "Very Light": "#e0ffe0", "Light": "#00ff01",
    "Moderate": "#00ffff", "Rather Heavy": "#ffeb3b", "Heavy": "#ff8c00",
    "Very Heavy": "#d50000", "Extremely Heavy": "#f820fe", "Exceptional": "#e8aaf5"
}

category_ranges = {
    "No Rain": "0 mm", "Very Light": "0.1 – 2.4 mm", "Light": "2.5 – 7.5 mm",
    "Moderate": "7.6 – 35.5 mm", "Rather Heavy": "35.6 – 64.4 mm",
    "Heavy": "64.5 – 124.4 mm", "Very Heavy": "124.5 – 244.4 mm",
    "Extremely Heavy": "244.5 – 350 mm", "Exceptional": "> 350 mm"
}

ordered_categories = [
    "No Rain", "Very Light", "Light", "Moderate", "Rather Heavy",
    "Heavy", "Very Heavy", "Extremely Heavy", "Exceptional"
]

def classify_rainfall(rainfall):
    """Classifies rainfall amount into predefined categories."""
    if pd.isna(rainfall) or rainfall == 0:
        return "No Rain"
    elif rainfall > 0 and rainfall <= 2.4:
        return "Very Light"
    elif rainfall <= 7.5:
        return "Light"
    elif rainfall <= 35.5:
        return "Moderate"
    elif rainfall <= 64.4:
        return "Rather Heavy"
    elif rainfall <= 124.4:
        return "Heavy"
    elif rainfall <= 244.4:
        return "Very Heavy"
    elif rainfall <= 350:
        return "Extremely Heavy"
    else:
        return "Exceptional"

# ---------------------------- DATA LOADING & PROCESSING ----------------------------
def load_sheet_data(sheet_name, tab_name):
    """Loads data from a Google Sheet tab into a DataFrame."""
    try:
        # Debugging print statements to show the exact names being used
        print(f"Attempting to open spreadsheet: '{sheet_name}'")
        print(f"Attempting to open worksheet: '{tab_name}'")

        client = get_gsheet_client()
        if client:
            sheet = client.open(sheet_name)
            worksheet = sheet.worksheet(tab_name)
            df = pd.DataFrame(worksheet.get_all_records())
            df.columns = df.columns.str.strip()
            if 'TOTAL' in df.columns:
                df.rename(columns={"DISTRICT": "District", "TALUKA": "Taluka", "TOTAL": "Total_mm"}, inplace=True)
            else:
                df.rename(columns={"DISTRICT": "District", "TALUKA": "Taluka"}, inplace=True)
            return df
        return pd.DataFrame()
    except gspread.exceptions.WorksheetNotFound:
        print(f"ERROR: Data sheet for '{tab_name}' not found.")
        return pd.DataFrame()
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"ERROR: Spreadsheet '{sheet_name}' not found.")
        return pd.DataFrame()
    except Exception as e:
        print(f"ERROR: Error loading data: {e}")
        return pd.DataFrame()

def correct_taluka_names(df):
    """Corrects known inconsistencies in taluka names."""
    taluka_name_mapping = {
        "Morbi": "Morvi", "Ahmedabad City": "Ahmadabad City", "Maliya Hatina": "Malia",
        "Shihor": "Sihor", "Dwarka": "Okhamandal", "Kalol(Gnr)": "Kalol",
    }
    df['Taluka'] = df['Taluka'].replace(taluka_name_mapping)
    return df

def correct_district_names(df):
    """Corrects known inconsistencies in district names."""
    district_name_mapping = {
        "Chhota Udepur": "Chhota Udaipur", "Dangs": "Dang",
        "Kachchh": "Kutch", "Mahesana": "Mehsana",
    }
    df['District'] = df['District'].replace(district_name_mapping)
    return df

def process_daily_data(df):
    """Processes daily data to compute metrics and prepare for plotting."""
    df = correct_taluka_names(df)
    if "Rain_Last_24_Hrs" in df.columns:
        df.rename(columns={"Rain_Last_24_Hrs": "Total_mm"}, inplace=True)

    required_cols = ["Total_mm", "Taluka", "District"]
    for col in required_cols:
        if col not in df.columns:
            return {"error": f"Required column '{col}' not found in the loaded data."}

    if 'Total_Rainfall' not in df.columns:
        df['Total_Rainfall'] = df['Total_mm'] * 1.5
    if 'Percent_Against_Avg' not in df.columns:
        df['Percent_Against_Avg'] = (df['Total_Rainfall'] / 700) * 100

    df["Total_mm"] = pd.to_numeric(df["Total_mm"], errors='coerce')
    df["Total_Rainfall"] = pd.to_numeric(df["Total_Rainfall"], errors='coerce')
    df["Percent_Against_Avg"] = pd.to_numeric(df["Percent_Against_Avg"], errors='coerce')

    df = correct_district_names(df)
    df['District'] = df['District'].astype(str).str.strip()
    
    # Calculate key metrics
    state_total_seasonal_avg = df["Total_Rainfall"].mean() if not df["Total_Rainfall"].isnull().all() else 0.0
    state_avg_24hr = df["Total_mm"].mean() if not df["Total_mm"].isnull().all() else 0.0
    
    highest_taluka_row = df.loc[df["Total_mm"].idxmax()] if not df["Total_mm"].isnull().all() else pd.Series({'Taluka': 'N/A', 'Total_mm': 0, 'District': 'N/A'})
    highest_district_row = df.groupby('District')['Total_mm'].mean().reset_index().sort_values(by='Total_mm', ascending=False).iloc[0] if not df["Total_mm"].isnull().all() else pd.Series({'District': 'N/A', 'Total_mm': 0})
    
    TOTAL_TALUKAS_GUJARAT = 251
    num_talukas_with_rain_today = df[df['Total_mm'] > 0].shape[0]
    
    # Prepare data for charts
    df_plot_daily = df.copy()
    df_plot_daily["Rainfall_Category"] = df_plot_daily["Total_mm"].apply(classify_rainfall)
    df_plot_daily["Rainfall_Category"] = pd.Categorical(
        df_plot_daily["Rainfall_Category"],
        categories=ordered_categories,
        ordered=True
    )

    district_rainfall_avg_df = df_plot_daily.groupby('District')['Total_mm'].mean().reset_index()
    district_rainfall_avg_df = district_rainfall_avg_df.rename(
        columns={'Total_mm': 'District_Avg_Rain_Last_24_Hrs'}
    )
    district_rainfall_avg_df["Rainfall_Category"] = district_rainfall_avg_df["District_Avg_Rain_Last_24_Hrs"].apply(classify_rainfall)
    district_rainfall_avg_df["Rainfall_Category"] = pd.Categorical(
        district_rainfall_avg_df["Rainfall_Category"],
        categories=ordered_categories,
        ordered=True
    )
    
    # Calculate state rainfall progress
    total_seasonal_avg_state = 850
    state_rainfall_progress_percentage = (state_total_seasonal_avg / total_seasonal_avg_state) * 100
    
    # Convert dataframes to JSON for frontend
    data_for_frontend = {
        'metrics': {
            'state_total_seasonal_avg': float(f"{state_total_seasonal_avg:.1f}"),
            'state_avg_24hr': float(f"{state_avg_24hr:.1f}"),
            'highest_taluka': highest_taluka_row.to_dict(),
            'highest_district': highest_district_row.to_dict(),
            'total_talukas_with_rain': num_talukas_with_rain_today,
            'total_talukas_gujarat': TOTAL_TALUKAS_GUJARAT,
            'state_rainfall_progress_percentage': float(f"{state_rainfall_progress_percentage:.1f}")
        },
        'choropleth_data_taluka': df_plot_daily[['Taluka', 'Total_mm', 'District']].to_dict('records'),
        'choropleth_data_district': district_rainfall_avg_df[['District', 'District_Avg_Rain_Last_24_Hrs']].to_dict('records'),
        'top_10_talukas': df_plot_daily.dropna(subset=['Total_mm']).sort_values(by='Total_mm', ascending=False).head(10).to_dict('records'),
        'daily_table': df_plot_daily[['District', 'Taluka', 'Total_mm', 'Total_Rainfall', 'Percent_Against_Avg']].sort_values(by="Total_mm", ascending=False).reset_index(drop=True).to_dict('records')
    }
    
    return data_for_frontend


def process_hourly_data(df):
    """Processes hourly data for trends and metrics."""
    df = correct_taluka_names(df)
    df.columns = df.columns.str.strip()
    
    time_slot_columns = [col for col in df.columns if "TO" in col and df[col].dtype in ['int64', 'float64', 'object']]
    time_slot_order = ['06TO08', '08TO10', '10TO12', '12TO14', '14TO16', '16TO18',
                       '18TO20', '20TO22', '22TO24', '24TO02', '02TO04', '04TO06']
    existing_order = [slot for slot in time_slot_order if slot in time_slot_columns]

    for col in existing_order:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df['Total_mm'] = df[existing_order].sum(axis=1)
    df_long = df.melt(
        id_vars=["District", "Taluka", "Total_mm"],
        value_vars=existing_order,
        var_name="Time Slot",
        value_name="Rainfall (mm)"
    ).dropna(subset=["Rainfall (mm)"])
    
    slot_labels = {
        "06TO08": "6–8 AM", "08TO10": "8–10 AM", "10TO12": "10–12 AM",
        "12TO14": "12–2 PM", "14TO16": "2–4 PM", "16TO18": "4–6 PM",
        "18TO20": "6–8 PM", "20TO22": "8–10 PM", "22TO24": "10–12 PM",
        "24TO02": "12–2 AM", "02TO04": "2–4 AM", "04TO06": "4–6 AM",
    }
    df_long['Time Slot Label'] = df_long['Time Slot'].map(slot_labels)
    
    # Calculate key metrics
    top_taluka_row = df.sort_values(by='Total_mm', ascending=False).iloc[0].to_dict() if not df['Total_mm'].dropna().empty else {'Taluka': 'N/A', 'Total_mm': 0}
    df_latest_slot = df_long[df_long['Time Slot'] == existing_order[-1]]
    top_latest = df_latest_slot.sort_values(by='Rainfall (mm)', ascending=False).iloc[0].to_dict() if not df_latest_slot['Rainfall (mm)'].dropna().empty else {'Taluka': 'N/A', 'Rainfall (mm)': 0}
    num_talukas_with_rain_hourly = df[df['Total_mm'] > 0].shape[0]

    return {
        'metrics': {
            'latest_slot_label': slot_labels[existing_order[-1]],
            'total_talukas_with_rain': num_talukas_with_rain_hourly,
            'top_taluka_total_rainfall': top_taluka_row,
            'top_latest_rainfall': top_latest
        },
        'long_data': df_long.to_dict('records'),
        'available_talukas': sorted(df_long['Taluka'].unique().tolist())
    }

# ---------------------------- FLASK ROUTES ----------------------------

@app.route('/')
def dashboard_page():
    """Main route that renders the HTML dashboard."""
    # This route only renders the HTML structure
    return render_template('dashboard.html')

@app.route('/api/daily_data', methods=['GET'])
def get_daily_data():
    """API endpoint to fetch and process daily rainfall data."""
    date_str = request.args.get('date', datetime.today().strftime('%Y-%m-%d'))
    try:
        selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        selected_month = selected_date.strftime("%B")
        selected_year = selected_date.strftime("%Y")

        daily_tab_name = f"master24hrs_{selected_date.strftime('%Y-%m-%d')}"
        daily_sheet_name = f"24HR_Rainfall_{selected_month}_{selected_year}"
        
        df = load_sheet_data(daily_sheet_name, daily_tab_name)
        
        if df.empty:
            return jsonify({'error': f"Daily data not available for {date_str}."}), 404
            
        data = process_daily_data(df)
        data['title'] = f"24 Hours Rainfall Summary ({(selected_date - timedelta(days=1)).strftime('%d-%m-%Y')} 06:00 AM to {selected_date.strftime('%d-%m-%Y')} 06:00 AM)"
        return jsonify(data)
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/hourly_data', methods=['GET'])
def get_hourly_data():
    """API endpoint to fetch and process hourly rainfall data."""
    date_str = request.args.get('date', datetime.today().strftime('%Y-%m-%d'))
    try:
        selected_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        selected_month = selected_date.strftime("%B")
        selected_year = selected_date.strftime("%Y")

        hourly_sheet_name = f"2HR_Rainfall_{selected_month}_{selected_year}"
        hourly_tab_name = f"2hrs_master_{selected_date.strftime('%Y-%m-%d')}"
        
        df = load_sheet_data(hourly_sheet_name, hourly_tab_name)
        
        if df.empty:
            return jsonify({'error': f"Hourly data not available for {date_str}."}), 404

        data = process_hourly_data(df)
        return jsonify(data)
    except ValueError:
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/geojson/<name>', methods=['GET'])
def get_geojson(name):
    """API endpoint to serve GeoJSON files."""
    filepath = os.path.join('data', name)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        return jsonify(geojson_data)
    except FileNotFoundError:
        return jsonify({'error': f'GeoJSON file "{name}" not found.'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Ensure data and templates directories exist for local testing
    os.makedirs('data', exist_ok=True)
    os.makedirs('templates', exist_ok=True)
    app.run(debug=True, port=8501)