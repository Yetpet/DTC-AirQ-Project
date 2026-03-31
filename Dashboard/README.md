# Air Quality Dashboard (Streamlit)

This folder contains a lightweight dashboard for the DTC-AirQ data pipeline. It reads from a source table (DBT outputs) and renders metrics + charts.

## Setup

1. Activate your virtualenv: `& .venv\Scripts\Activate.ps1`
2. Install dependencies:
   - `pip install -r Dashboard/requirements.txt`

3. Set database URL (SQLAlchemy format):

```powershell
$Env:DB_URL = "postgresql://user:pass@host:5432/db"
# or BigQuery
$Env:DB_URL = "bigquery://aq_data_lake_air-quality-project-491604.air_quality_dataset"
```

## Run

```powershell
streamlit run Dashboard/airquality_app.py
```

## Data table names

- `fct_daily_air_quality_summary`
- `fct_air_quality_measurements`
- `city_air_quality_rankings`
- `forecast_accuracy_analysis`

Optional upload mode available in GUI for CSV files.

## Customization

- Adjust SQL queries in `airquality_app.py` to point to your exact dataset/schema.
- Add more visuals and filters as needed.

## Alternative Dashboard Option

For a no-code BI dashboard, consider using Google Looker Studio (formerly Data Studio) at [datastudio.google.com](https://datastudio.google.com). Connect directly to your BigQuery dataset for visualizations without coding.
