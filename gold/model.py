import pyspark.pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Creating dimenstion of passengers
@dp.view
def dim_passenger_view():
    df = spark.readStream.table("silver_obt")
    df = df.select(col("passenger_id"),col("passenger_name"),col("passenger_email"),col("passenger_phone"))
    df = df.dropDuplicates(subset = ['passenger_id'])
    return df


dp.create_streaming_table("dim_passengers")
dp.create_auto_cdc_flow(
  target = "dim_passengers",
  source = "dim_passenger_view",
  keys = ["passenger_id"],
  sequence_by = col("passenger_id"),
  stored_as_scd_type = "1"
) 


# Creating dim_drivers
@dp.view
def dim_driver_view():
    df = spark.readStream.table("silver_obt")
    df = df.select(col("driver_id"),col("driver_name"),col("driver_rating"),col("driver_phone"),col("driver_license"))
    df = df.dropDuplicates(subset = ['driver_id'])
    return df

dp.create_streaming_table("dim_drivers")
dp.create_auto_cdc_flow(
  target = "dim_drivers",
  source = "dim_driver_view",
  keys = ["driver_id"],
  sequence_by = col("driver_id"),
  stored_as_scd_type = "1"
) 

# dim_vehicle
@dp.view
def dim_vehicle_view():
    df = spark.readStream.table("silver_obt")
    df = df.select(col("vehicle_id"),col("vehicle_type_id"),col("vehicle_make_id"),col("vehicle_model"),col("vehicle_color"),col("license_plate"),col("vehicle_make"),col("vehicle_type"))
    df = df.dropDuplicates(subset = ['vehicle_id'])
    return df

dp.create_streaming_table("dim_vehicles")
dp.create_auto_cdc_flow(
  target = "dim_vehicles",
  source = "dim_vehicle_view",
  keys = ["vehicle_id"],
  sequence_by = col("vehicle_id"),
  stored_as_scd_type = "1"
)


@dp.view
def dim_payment_view():
    df = spark.readStream.table("silver_obt")
    df = df.select(col("payment_method_id"),col("payment_method"),col("is_card"),col("requires_auth"))
    df = df.dropDuplicates(subset = ['payment_method_id'])
    return df

dp.create_streaming_table("dim_payments")
dp.create_auto_cdc_flow(
  target = "dim_payments",
  source = "dim_payment_view",
  keys = ["payment_method_id"],
  sequence_by = col("payment_method_id"),
  stored_as_scd_type = "1"
)

# dim_bookings
@dp.view
def dim_booking_view():
    df = spark.readStream.table("silver_obt")
    df = df.select(col("ride_id"),col("confirmation_number"),col("dropoff_location_id"),col("ride_status_id"),col("dropoff_city_id"),col("cancellation_reason_id"),col("dropoff_address"),col("dropoff_latitude"),col("dropoff_longitude"),col("booking_timestamp"),col("dropoff_timestamp"),col("pickup_latitude"),col("pickup_longitude"),col("pickup_location_id"),col("pickup_timestamp"))
    df = df.dropDuplicates(subset = ['ride_id'])
    return df

dp.create_streaming_table("dim_bookings")
dp.create_auto_cdc_flow(
  target = "dim_bookings",
  source = "dim_booking_view",
  keys = ["ride_id"],
  sequence_by = col("ride_id"),
  stored_as_scd_type = "1"
)

# dim_pichup_city
@dp.view
def dim_pickup_city_view():
    df = spark.readStream.table("silver_obt")
    df = df.select(col("pickup_city_id"),col("pickup_city"),col("region"),col("state"),col("pickup_address"),col("updated_at"))
    df = df.dropDuplicates(subset = ['pickup_city_id','updated_at'])
    return df

dp.create_streaming_table("dim_pickup_city")
dp.create_auto_cdc_flow(
  target = "dim_pickup_city",
  source = "dim_pickup_city_view",
  keys = ["pickup_city_id"],
  sequence_by = col("updated_at"),
  stored_as_scd_type = "2"
)


# act Table
# always end _END_AT IS NULL when joining with the dimention tables
@dp.view
def fact_table_view():
  df = spark.readStream.table("silver_obt")
  df = df.select("distance_miles","duration_minutes","base_fare","distance_fare","time_fare","surge_multiplier","subtotal","tip_amount","total_fare","rating","base_rate","per_mile","per_minute","ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id")
  df.dropDuplicates(subset = ['ride_id'])
  return df

dp.create_streaming_table("fact_table")
dp.create_auto_cdc_flow(
  target = 'fact_table',
  source = 'fact_table_view',
  keys = ["ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id"],
  sequence_by = col("ride_id"),
  stored_as_scd_type = "1"
)


