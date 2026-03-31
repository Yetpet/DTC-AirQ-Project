# Air Quality DBT Transformations

This DBT project transforms raw air quality data from OpenWeatherMap API into analytics-ready datasets for air quality monitoring and analysis.

## Project Structure

```
air_quality_dbt/
├── models/
│   ├── sources.yml                    # BigQuery external table definitions
│   ├── staging/                       # Raw data cleaning and standardization
│   │   ├── stg_air_quality_historical.sql
│   │   ├── stg_air_quality_current.sql
│   │   └── stg_air_quality_forecast.sql
│   ├── intermediate/                  # Business logic and enrichment
│   │   ├── int_air_quality_unified.sql
│   │   └── int_air_quality_daily_summary.sql
│   └── marts/                         # Final analytics datasets
│       ├── air_quality/
│       │   ├── fct_air_quality_measurements.sql
│       │   └── fct_daily_air_quality_summary.sql
│       └── analytics/
│           ├── city_air_quality_rankings.sql
│           ├── temporal_air_quality_patterns.sql
│           └── forecast_accuracy_analysis.sql
├── dbt_project.yml                   # DBT project configuration
└── profiles.yml                      # Connection profiles
```

## Key Transformations

### 1. **Air Quality Categorization**
- EPA-standard AQI categories (Good, Moderate, Unhealthy, etc.)
- Health impact levels and pollution severity ratings
- Primary pollutant identification

### 2. **Time-Series Analysis**
- Hourly, daily, and monthly aggregations
- Seasonal patterns and temporal trends
- Weekday vs weekend comparisons

### 3. **Geographic Analytics**
- City-level rankings and comparisons
- Country-level performance metrics
- Cross-regional air quality analysis

### 4. **Forecast Accuracy**
- Prediction vs actual measurement comparison
- Forecast reliability scoring
- Lead time impact analysis

### 5. **Health & Environmental Indicators**
- Pollution load indices
- Health risk assessments
- Data quality monitoring

## Usage

### Setup
```bash
cd Transformtatn/air_quality_dbt
uv run dbt deps
uv run dbt debug
```

### Run Transformations
```bash
# Run all models
uv run dbt run

# Run specific model
uv run dbt run --select fct_air_quality_measurements

# Run by layer
uv run dbt run --select staging
uv run dbt run --select marts
```

### Test Data Quality
```bash
uv run dbt test
```

### Generate Documentation
```bash
uv run dbt docs generate
uv run dbt docs serve
```

## Data Flow

1. **Raw Data** → External tables in BigQuery
2. **Staging** → Data cleaning, type conversion, basic validation
3. **Intermediate** → Business logic, categorization, aggregations
4. **Marts** → Analytics-ready datasets for reporting and dashboards

## Key Metrics

- **AQI Categories**: EPA-standard air quality classifications
- **Pollution Load Index**: Weighted average of all pollutants
- **Forecast Accuracy**: Prediction reliability scoring
- **Health Risk Levels**: Population health impact assessments
- **Data Completeness**: Quality monitoring metrics

## Business Applications

- **Environmental Monitoring**: Track air quality trends and patterns
- **Public Health**: Identify high-risk areas and time periods
- **Policy Making**: Support air quality regulations and interventions
- **Urban Planning**: Guide city development and traffic management
- **Forecast Validation**: Monitor and improve prediction accuracy