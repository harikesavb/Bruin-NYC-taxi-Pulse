/* @bruin

name: reports.trips_daily_report
type: duckdb.sql
depends:
  - staging.trips_clean

materialization:
  type: table

columns:
  - name: trip_date
    type: date
    description: Report date.
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Taxi service segment.
    primary_key: true
    checks:
      - name: accepted_values
        value: [yellow, green]
  - name: trip_count
    type: integer
    description: Number of valid trips.
    checks:
      - name: non_negative
  - name: total_passengers
    type: integer
    description: Total riders across trips.
    checks:
      - name: non_negative
  - name: total_distance_miles
    type: float
    description: Total miles traveled.
    checks:
      - name: non_negative
  - name: avg_trip_duration_minutes
    type: float
    description: Average trip duration.
    checks:
      - name: non_negative
  - name: avg_speed_mph
    type: float
    description: Blended average speed.
    checks:
      - name: non_negative
  - name: gross_fare_amount
    type: float
    description: Total fare before tips.
    checks:
      - name: non_negative
  - name: total_tip_amount
    type: float
    description: Total tips collected.
    checks:
      - name: non_negative
  - name: tip_rate_pct_card
    type: float
    description: Tip percentage on card-paid fares.
    checks:
      - name: non_negative
  - name: total_revenue_amount
    type: float
    description: Total collected amount.
    checks:
      - name: non_negative
  - name: cash_trip_count
    type: integer
    description: Number of cash trips.
    checks:
      - name: non_negative
  - name: card_trip_count
    type: integer
    description: Number of card trips.
    checks:
      - name: non_negative
  - name: card_trip_share_pct
    type: float
    description: Share of trips paid by card.
    checks:
      - name: non_negative
      - name: max
        value: 100

custom_checks:
  - name: report_not_empty
    description: Daily report should contain at least one row.
    query: |
      SELECT count(*) > 0
      FROM reports.trips_daily_report
    value: 1
  - name: unique_day_taxi_grain
    description: Only one row per (trip_date, taxi_type).
    query: |
      SELECT count(*)
      FROM (
        SELECT trip_date, taxi_type, count(*) AS row_count
        FROM reports.trips_daily_report
        GROUP BY 1, 2
        HAVING count(*) > 1
      ) duplicates
    value: 0
  - name: card_plus_cash_not_above_total_trips
    description: Card and cash counts cannot exceed total trips.
    query: |
      SELECT count(*)
      FROM reports.trips_daily_report
      WHERE card_trip_count + cash_trip_count > trip_count
    value: 0
  - name: revenue_not_below_fare
    description: Total revenue should be at least fare amount.
    query: |
      SELECT count(*)
      FROM reports.trips_daily_report
      WHERE total_revenue_amount < gross_fare_amount
    value: 0

@bruin */

WITH daily_agg AS (
  SELECT
    trip_date,
    taxi_type,
    COUNT(*) AS trip_count,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance_miles) AS total_distance_miles,
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
    SUM(trip_duration_minutes) AS total_trip_minutes,
    SUM(fare_amount) AS gross_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(total_amount) AS total_revenue_amount,
    SUM(CASE WHEN payment_type_name = 'Cash' THEN 1 ELSE 0 END) AS cash_trip_count,
    SUM(CASE WHEN payment_type_name = 'Credit card' THEN 1 ELSE 0 END) AS card_trip_count,
    SUM(CASE WHEN payment_type_name = 'Credit card' THEN tip_amount ELSE 0 END) AS card_tip_amount,
    SUM(CASE WHEN payment_type_name = 'Credit card' THEN fare_amount ELSE 0 END) AS card_fare_amount
  FROM staging.trips_clean
  GROUP BY 1, 2
)
SELECT
  trip_date,
  taxi_type,
  trip_count,
  total_passengers,
  ROUND(total_distance_miles, 2) AS total_distance_miles,
  ROUND(avg_trip_duration_minutes, 2) AS avg_trip_duration_minutes,
  ROUND(total_distance_miles / NULLIF(total_trip_minutes, 0) * 60, 2) AS avg_speed_mph,
  ROUND(gross_fare_amount, 2) AS gross_fare_amount,
  ROUND(total_tip_amount, 2) AS total_tip_amount,
  ROUND(CASE WHEN card_fare_amount = 0 THEN 0 ELSE 100 * card_tip_amount / card_fare_amount END, 2) AS tip_rate_pct_card,
  ROUND(total_revenue_amount, 2) AS total_revenue_amount,
  cash_trip_count,
  card_trip_count,
  ROUND(100.0 * card_trip_count / NULLIF(trip_count, 0), 2) AS card_trip_share_pct
FROM daily_agg
ORDER BY trip_date, taxi_type;
