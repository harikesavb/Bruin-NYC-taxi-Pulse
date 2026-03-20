/* @bruin

name: staging.trips_clean
type: duckdb.sql
depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

columns:
  - name: trip_id
    type: integer
    description: Deduplicated trip identifier retained in staging.
    checks:
      - name: not_null
      - name: unique
  - name: trip_date
    type: date
    description: Pickup date for reporting grain.
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: NYC taxi service type.
    checks:
      - name: accepted_values
        value: [yellow, green]
  - name: pickup_datetime
    type: timestamp
    description: Pickup timestamp.
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Dropoff timestamp.
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: Number of passengers.
    checks:
      - name: positive
  - name: trip_distance_miles
    type: float
    description: Cleaned trip distance in miles.
    checks:
      - name: non_negative
  - name: trip_duration_minutes
    type: integer
    description: Duration in minutes.
    checks:
      - name: positive
  - name: avg_speed_mph
    type: float
    description: Average speed in miles per hour.
    checks:
      - name: non_negative
  - name: payment_type_name
    type: string
    description: Payment method label from lookup.
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: Metered fare amount in USD.
    checks:
      - name: non_negative
  - name: tip_amount
    type: float
    description: Tip amount in USD.
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: Total paid amount in USD.
    checks:
      - name: non_negative
  - name: tip_rate_pct
    type: float
    description: Tip as a percentage of fare.
    checks:
      - name: non_negative

custom_checks:
  - name: no_unmapped_payment_type
    description: Every staged trip should map to a known payment label.
    query: |
      SELECT count(*)
      FROM staging.trips_clean
      WHERE payment_type_name IS NULL
    value: 0
  - name: no_outlier_speeds_over_70_mph
    description: Urban taxi trips above 70 mph are treated as bad records.
    query: |
      SELECT count(*)
      FROM staging.trips_clean
      WHERE avg_speed_mph > 70
    value: 0
  - name: no_trips_longer_than_3_hours
    description: Trips over 180 minutes are excluded from this model.
    query: |
      SELECT count(*)
      FROM staging.trips_clean
      WHERE trip_duration_minutes > 180
    value: 0

@bruin */

WITH raw_trips AS (
  SELECT
    CAST(trip_id AS BIGINT) AS trip_id,
    LOWER(TRIM(taxi_type)) AS taxi_type,
    CAST(vendor_id AS INTEGER) AS vendor_id,
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(pickup_location_id AS INTEGER) AS pickup_location_id,
    CAST(dropoff_location_id AS INTEGER) AS dropoff_location_id,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance_miles AS DOUBLE) AS trip_distance_miles,
    CAST(payment_type_id AS INTEGER) AS payment_type_id,
    CAST(fare_amount AS DOUBLE) AS fare_amount,
    CAST(tip_amount AS DOUBLE) AS tip_amount,
    CAST(tolls_amount AS DOUBLE) AS tolls_amount,
    CAST(total_amount AS DOUBLE) AS total_amount
  FROM ingestion.trips
),
filtered_trips AS (
  SELECT *
  FROM raw_trips
  WHERE taxi_type IN ('yellow', 'green')
    AND dropoff_datetime > pickup_datetime
    AND passenger_count BETWEEN 1 AND 6
    AND trip_distance_miles > 0
    AND trip_distance_miles <= 80
    AND fare_amount >= 0
    AND tip_amount >= 0
    AND tolls_amount >= 0
    AND total_amount >= fare_amount
),
deduped_trips AS (
  SELECT
    trip_id,
    taxi_type,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance_miles,
    payment_type_id,
    fare_amount,
    tip_amount,
    total_amount
  FROM (
    SELECT
      ft.*,
      ROW_NUMBER() OVER (
        PARTITION BY
          ft.taxi_type,
          ft.pickup_datetime,
          ft.dropoff_datetime,
          ft.pickup_location_id,
          ft.dropoff_location_id,
          ROUND(ft.total_amount, 2)
        ORDER BY ft.trip_id DESC
      ) AS rn
    FROM filtered_trips AS ft
  ) AS ranked
  WHERE rn = 1
),
enriched_trips AS (
  SELECT
    d.trip_id,
    CAST(d.pickup_datetime AS DATE) AS trip_date,
    d.taxi_type,
    d.pickup_datetime,
    d.dropoff_datetime,
    d.passenger_count,
    ROUND(d.trip_distance_miles, 2) AS trip_distance_miles,
    DATEDIFF('minute', d.pickup_datetime, d.dropoff_datetime) AS trip_duration_minutes,
    p.payment_type_name,
    ROUND(d.fare_amount, 2) AS fare_amount,
    ROUND(d.tip_amount, 2) AS tip_amount,
    ROUND(d.total_amount, 2) AS total_amount
  FROM deduped_trips AS d
  LEFT JOIN ingestion.payment_lookup AS p
    ON d.payment_type_id = p.payment_type_id
)
SELECT
  e.trip_id,
  e.trip_date,
  e.taxi_type,
  e.pickup_datetime,
  e.dropoff_datetime,
  e.passenger_count,
  e.trip_distance_miles,
  e.trip_duration_minutes,
  ROUND(e.trip_distance_miles / NULLIF(e.trip_duration_minutes / 60.0, 0), 2) AS avg_speed_mph,
  e.payment_type_name,
  e.fare_amount,
  e.tip_amount,
  e.total_amount,
  ROUND(CASE WHEN e.fare_amount = 0 THEN 0 ELSE 100 * e.tip_amount / e.fare_amount END, 2) AS tip_rate_pct
FROM enriched_trips AS e
WHERE e.trip_duration_minutes BETWEEN 1 AND 180
  AND (e.trip_distance_miles / NULLIF(e.trip_duration_minutes / 60.0, 0)) <= 70;
