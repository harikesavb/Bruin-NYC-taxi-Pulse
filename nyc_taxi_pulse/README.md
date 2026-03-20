# NYC Taxi Pulse

NYC Taxi Pulse is a Bruin + DuckDB pipeline that ingests NYC TLC trip data, cleans it, and creates daily taxi KPIs.

## Data Source

Public NYC TLC parquet files:

`https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{YYYY-MM}.parquet`

Supported taxi types:

- `yellow`
- `green`

## Project Layout

```text
nyc_taxi_pulse/
  pipeline/
    pipeline.yml
    assets/
      ingestion/
      staging/
      reports/
```

## Step-by-Step: Run in Terminal

1. Open terminal and go to the project root.

```powershell
cd C:\Users\Harikesava\Mine\my-first-pipeline\bruin
```

2. Validate pipeline config.

```powershell
bruin validate ./nyc_taxi_pulse/pipeline/pipeline.yml --environment default
```

3. Run for a month (default taxi type = `yellow`).

```powershell
bruin run ./nyc_taxi_pulse/pipeline/pipeline.yml --environment default --start-date 2024-01-01 --end-date 2024-01-31
```

4. Run with both taxi types.

```powershell
$env:BRUIN_VARS = (@{ taxi_types = @("yellow","green") } | ConvertTo-Json -Compress)
bruin run ./nyc_taxi_pulse/pipeline/pipeline.yml --environment default --start-date 2024-01-01 --end-date 2024-01-31
```

5. Optional clean rebuild.

```powershell
bruin run ./nyc_taxi_pulse/pipeline/pipeline.yml --environment default --full-refresh --start-date 2024-01-01 --end-date 2024-01-31
```

## Date Window Examples

Single day:

```powershell
bruin run ./nyc_taxi_pulse/pipeline/pipeline.yml --environment default --start-date 2024-01-01 --end-date 2024-01-01
```

Single month:

```powershell
bruin run ./nyc_taxi_pulse/pipeline/pipeline.yml --environment default --start-date 2024-03-01 --end-date 2024-03-31
```

Full year:

```powershell
bruin run ./nyc_taxi_pulse/pipeline/pipeline.yml --environment default --start-date 2025-01-01 --end-date 2025-12-31
```

## Verify Output (Optional)

```powershell
@'
import duckdb
con = duckdb.connect(r'./nyc_taxi_pulse/nyc_taxi_pulse.duckdb')
print(con.execute('SELECT COUNT(*) FROM ingestion.trips').fetchone())
print(con.execute('SELECT COUNT(*) FROM staging.trips_clean').fetchone())
print(con.execute('SELECT COUNT(*) FROM reports.trips_daily_report').fetchone())
'@ | python -
```

## Notes

- Pipeline schedule is `daily` in `pipeline/pipeline.yml`.
- `taxi_types` defaults to `["yellow"]`.
- Reset taxi type override when needed:

```powershell
Remove-Item Env:BRUIN_VARS
```
