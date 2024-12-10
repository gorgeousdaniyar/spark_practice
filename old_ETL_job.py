import spark as spark
from pyspark.sql import SparkSession
import os
import requests
import geohash2  # Using geohash2 to create geohashes
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame

# Stop the active SparkSession if there is one
existing_spark = SparkSession.getActiveSession()
if existing_spark:
    existing_spark.stop()

# Create a new Spark session with specific memory settings
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("RestaurantETL") \
    .getOrCreate()

# Paths to the datasets
restaurant_data_path = r"/Users/gorgeousdan/Documents/projects/intellijidea/spark_practice/Dataset_Restaurants"
weather_data_path = r"/Users/gorgeousdan/Documents/projects/intellijidea/spark_practice/Dataset_Weather"

# Find all restaurant CSV files recursively
restaurant_files = [
    os.path.join(dp, f) for dp, dn, filenames in os.walk(restaurant_data_path) for f in filenames if f.endswith('.csv')
]

# Find all weather Parquet files recursively
weather_files = [
    os.path.join(dp, f) for dp, dn, filenames in os.walk(weather_data_path) for f in filenames if f.endswith('.parquet')
]

# Load restaurant data
restaurant_data = spark.read.csv(restaurant_files, header=True, inferSchema=True)

# Load weather data
weather_data = spark.read.parquet(*weather_files)

# Check the schema of restaurant data
print("Restaurant data schema:")
restaurant_data.printSchema()

# Check the schema of weather data
print("Weather data schema:")
weather_data.printSchema()

# Show first rows of restaurant data
print("Sample restaurant data:")
restaurant_data.show(5)

# Show first rows of weather data
print("Sample weather data:")
weather_data.show(5)

# Step 1: Filter out rows with null coordinates
invalid_coords = restaurant_data.filter((restaurant_data.lat.isNull()) | (restaurant_data.lng.isNull()))

# Show rows with null coordinates
print("Rows with invalid coordinates:")
invalid_coords.show()

# OpenCage API key
API_KEY = "3c34740e32cb43bc8aba0823f126396f"

# Function to get coordinates using the OpenCage API
def get_coordinates(city, country, api_key):
    url = "https://api.opencagedata.com/geocode/v1/json"
    params = {
        "q": f"{city}, {country}",
        "key": api_key
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            coords = data['results'][0]['geometry']
            return coords['lat'], coords['lng']
    return None, None

# Function to update coordinates for rows with missing values
def update_coordinates(row):
    if row.lat is None or row.lng is None:
        print(f"Checking coordinates for city: {row.city}, {row.country}...")
        lat, lng = get_coordinates(row.city, row.country, API_KEY)
        print(f"API response: lat={lat}, lng={lng}")
        return (row.id, row.franchise_id, row.franchise_name, row.restaurant_franchise_id, row.country, row.city, lat, lng)
    else:
        return (row.id, row.franchise_id, row.franchise_name, row.restaurant_franchise_id, row.country, row.city, row.lat, row.lng)

# Update coordinates and convert to DataFrame
updated_coords_rdd = invalid_coords.rdd.map(update_coordinates)
updated_coords_df = updated_coords_rdd.toDF(['id', 'franchise_id', 'franchise_name', 'restaurant_franchise_id', 'country', 'city', 'lat', 'lng'])

# Combine updated rows with the valid ones
remaining_valid_coords = restaurant_data.filter((restaurant_data.lat.isNotNull()) & (restaurant_data.lng.isNotNull()))
restaurant_data = remaining_valid_coords.union(updated_coords_df)

# Function to generate geohashes
def generate_geohash(lat, lng):
    if lat is not None and lng is not None:
        return geohash2.encode(lat, lng, precision=4)  # Using geohash2
    return None

# Register UDF to generate geohashes
generate_geohash_udf = udf(generate_geohash, StringType())

# Add geohashes to restaurant and weather data
restaurant_data_with_geohash = restaurant_data.withColumn("geohash", generate_geohash_udf(restaurant_data.lat, restaurant_data.lng))
weather_data_with_geohash = weather_data.withColumn("geohash", generate_geohash_udf(weather_data.lat, weather_data.lng))

# Check updated DataFrames
print("Restaurant data with geohashes:")
restaurant_data_with_geohash.show()

print("Weather data with geohashes:")
weather_data_with_geohash.show()

# Function to safely perform a left join
def perform_left_join(restaurants: DataFrame, weather: DataFrame, output_path: str) -> None:
    # Remove duplicates in weather data
    weather_deduplicated = weather.dropDuplicates(["geohash"])

    # Perform the left join
    enriched_data = restaurants.join(
        weather_deduplicated.drop("lat", "lng"),  # Remove unnecessary columns
        on="geohash",
        how="left"
    )

    # Show the joined data
    print("Sample of joined data:")
    enriched_data.show(5, truncate=False)

    # Save the joined data
    enriched_data.write.mode("overwrite").parquet(output_path)
    print(f"Joined data saved at {output_path}")

    # Run tests
    print("Running validation tests...")

    # Test 1: Check no data multiplication for restaurants
    original_restaurant_count = restaurants.select("id").distinct().count()
    joined_restaurant_count = enriched_data.select("id").distinct().count()
    assert original_restaurant_count == joined_restaurant_count, "Data was multiplied during the join!"

    # Test 2: Check all fields are present
    required_fields = set(restaurants.columns + weather.columns) - {"lat", "lng"}
    result_fields = set(enriched_data.columns)
    missing_fields = required_fields - result_fields
    assert not missing_fields, f"Missing fields: {missing_fields}"

    # Test 3: Ensure idempotency
    rejoined_data = enriched_data.join(
        weather_deduplicated.drop("lat", "lng"),
        on="geohash",
        how="left"
    )
    assert enriched_data.count() == rejoined_data.count(), "Join is not idempotent!"

    print("All tests passed!")

# Call the function to join and save data
enriched_output_path = r"/Users/gorgeousdan/Documents/projects/intellijidea/spark_practice/enriched_data.parquet"
perform_left_join(restaurant_data_with_geohash, weather_data_with_geohash, enriched_output_path)

# Stop the Spark session
spark.stop()