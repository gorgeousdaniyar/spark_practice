# Spark Practice Project - EPAM + TechOrda Spark Training

This project is part of the **EPAM + TechOrda Spark Training** program and demonstrates a complete ETL pipeline using Apache Spark. The goal was to perform data enrichment and save the results in an optimized format while maintaining best practices for processing and testing.

## Objective

1. **Perform a left join of weather and restaurant data**:
   - Join datasets using geohash as a common key.
   - Ensure no data multiplication during the join.
   - Maintain an idempotent ETL process.
   
2. **Save the enriched data**:
   - Store the results in **Parquet format** on the local filesystem.
   - Preserve partitioning for efficient processing.

## Project Files and Structure

- **`enriched_data.parquet/`**: Directory containing the output of the ETL pipeline. This is the enriched dataset in Parquet format.
- **`MamenovD_output_spark_practice.pdf`**: Captures the console output of the ETL process, including schema validation, data previews, and test results.
- **`old_ETL_job.py`**: The main ETL script written in Python with PySpark.
- **`README.md`**: This documentation file, providing an overview of the project.

## Data Sources

- **Restaurant Data**:
  - Format: CSV
  - Description: Contains restaurant details, including IDs, franchise information, and coordinates (latitude/longitude).
  
- **Weather Data**:
  - Format: Parquet
  - Description: Contains weather details like average temperatures (Celsius/Fahrenheit) and coordinates (latitude/longitude).

## Key Steps in the ETL Process

1. **Load Data**:
   - Read restaurant data from CSV files and weather data from Parquet files.

2. **Handle Missing Data**:
   - Use the OpenCage API to fetch missing latitude and longitude for restaurants.

3. **Generate Geohashes**:
   - Compute geohash values for both datasets based on latitude and longitude.

4. **Join Datasets**:
   - Perform a left join of restaurant and weather data using geohash as the key.

5. **Save Enriched Data**:
   - Save the output as a Parquet file in the `enriched_data.parquet/` directory.

6. **Validation Tests**:
   - Check for data integrity, idempotency, and field completeness.

## Running the Project

### Prerequisites

- Python 3.10 or higher.
- Apache Spark installed and configured.
- Required Python libraries (`pyspark`, `geohash2`, `requests`) installed in a virtual environment.

### Steps

1. Clone the repository:
   ```bash
   git clone <repository_url>
   cd spark_practice
   
2.	Run the ETL script:
   python old_ETL_job.py
  	
3.	Review the results:
	•	Enriched data will be saved in the enriched_data.parquet/ directory.
	•	Console logs can be found in MamenovD_output_spark_practice.pdf.
   
