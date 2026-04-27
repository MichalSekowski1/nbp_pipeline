"""
Silver Layer: NBP Exchange Rates Transformation
Consumes bronze data, deduplicates, and creates clean silver table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NBP Silver Layer Transformation") \
    .getOrCreate()

# Table configuration
CATALOG = 'workspace'
BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'
BRONZE_TABLE = 'nbp_exchange_rates_bronze'
SILVER_TABLE = 'nbp_exchange_rates_silver'

BRONZE_FULL_NAME = f"{CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"
SILVER_FULL_NAME = f"{CATALOG}.{SILVER_SCHEMA}.{SILVER_TABLE}"

# Columns to keep in silver layer
SILVER_COLUMNS = ['currency_code', 'effectiveDate', 'mid']

print(f"Source table: {BRONZE_FULL_NAME}")
print(f"Target table: {SILVER_FULL_NAME}")
print(f"Spark version: {spark.version}")


def read_bronze_data(table_name):
    """
    Read data from bronze layer table.
    
    Args:
        table_name: Fully qualified bronze table name
    
    Returns:
        PySpark DataFrame with bronze data
    """
    print(f"\nReading data from {table_name}...")
    
    df = spark.table(table_name)
    record_count = df.count()
    
    print(f"✓ Loaded {record_count} records from bronze layer")
    
    return df


def deduplicate_data(df):
    """
    Deduplicate records based on currency_code + effectiveDate.
    For duplicates, keep the record with the latest ingestion_timestamp.
    
    Args:
        df: Input DataFrame with potential duplicates
    
    Returns:
        Deduplicated DataFrame
    """
    print("\nDeduplicating data...")
    
    initial_count = df.count()
    
    # Define window partitioned by currency_code and effectiveDate
    # Order by ingestion_timestamp descending to get the latest record first
    window_spec = Window.partitionBy('currency_code', 'effectiveDate') \
                        .orderBy(col('ingestion_timestamp').desc())
    
    # Add row number within each partition
    df_with_row_num = df.withColumn('row_num', row_number().over(window_spec))
    
    # Keep only the first row (latest ingestion_timestamp) in each partition
    df_deduplicated = df_with_row_num.filter(col('row_num') == 1).drop('row_num')
    
    final_count = df_deduplicated.count()
    duplicates_removed = initial_count - final_count
    
    print(f"  Initial records: {initial_count}")
    print(f"  Deduplicated records: {final_count}")
    print(f"  Duplicates removed: {duplicates_removed}")
    
    if duplicates_removed > 0:
        print(f"  ⚠ Removed {duplicates_removed} duplicate records ({(duplicates_removed/initial_count*100):.2f}%)")
    else:
        print(f"  ✓ No duplicates found")
    
    return df_deduplicated


def select_silver_columns(df, columns):
    """
    Select only the columns needed for silver layer.
    
    Args:
        df: Input DataFrame
        columns: List of column names to keep
    
    Returns:
        DataFrame with selected columns
    """
    print(f"\nSelecting silver layer columns: {', '.join(columns)}")
    
    df_selected = df.select(*columns)
    
    print("✓ Column selection complete")
    
    return df_selected


def add_processing_metadata(df):
    """
    Add silver layer processing metadata.
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with metadata columns added
    """
    print("\nAdding processing metadata...")
    
    df_with_metadata = df.withColumn("processing_timestamp", current_timestamp())
    
    print("✓ Added processing_timestamp column")
    
    return df_with_metadata


def validate_silver_data(df):
    """
    Validate silver layer data quality.
    
    Args:
        df: DataFrame to validate
    
    Returns:
        Boolean indicating if validation passed
    """
    print("\n" + "="*60)
    print("SILVER LAYER DATA VALIDATION")
    print("="*60 + "\n")
    
    total_records = df.count()
    issues = []
    
    # Check 1: Verify no nulls in critical columns
    print("Check 1: Null Value Validation")
    for col_name in ['currency_code', 'effectiveDate', 'mid']:
        null_count = df.filter(col(col_name).isNull()).count()
        status = "✓" if null_count == 0 else "✗"
        print(f"  {status} {col_name}: {null_count} nulls")
        
        if null_count > 0:
            issues.append(f"Found {null_count} nulls in {col_name}")
    
    # Check 2: Verify no duplicates exist
    print("\nCheck 2: Duplicate Verification")
    duplicate_check = df.groupBy('currency_code', 'effectiveDate').count() \
                        .filter(col('count') > 1).count()
    
    status = "✓" if duplicate_check == 0 else "✗"
    print(f"  {status} Duplicate check: {duplicate_check} duplicates found")
    
    if duplicate_check > 0:
        issues.append(f"Found {duplicate_check} duplicate records after deduplication")
    
    # Check 3: Record count by currency
    print("\nCheck 3: Record Distribution")
    currency_counts = df.groupBy('currency_code').count().orderBy('currency_code').collect()
    
    for row in currency_counts:
        print(f"  • {row['currency_code']}: {row['count']} records")
    
    # Summary
    print("\n" + "="*60)
    if issues:
        print("✗ VALIDATION FAILED")
        for issue in issues:
            print(f"  ✗ {issue}")
        print("="*60 + "\n")
        return False
    else:
        print("✓ VALIDATION PASSED")
        print(f"Total Records: {total_records}")
        print("="*60 + "\n")
        return True


def write_to_silver_table(df, table_name, write_mode='overwrite'):
    """
    Write transformed data to silver layer table.
    
    Args:
        df: DataFrame to write
        table_name: Fully qualified silver table name
        write_mode: 'overwrite' or 'append'
    """
    print(f"Writing to {table_name} (mode: {write_mode})...")
    
    record_count = df.count()
    
    print("\nFinal Schema:")
    df.printSchema()
    
    print("\nSample data (first 5 rows):")
    df.show(5, truncate=False)
    
    # Write to Delta table
    df.write \
        .format("delta") \
        .mode(write_mode) \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    
    print(f"✓ Successfully wrote {record_count} records to {table_name}")
    
    # Verify write
    result_count = spark.table(table_name).count()
    print(f"✓ Table now contains {result_count} total records")


def main():
    """
    Main execution function for silver layer transformation
    """
    try:
        print("\n" + "="*60)
        print("SILVER LAYER TRANSFORMATION - START")
        print("="*60)
        
        # Step 1: Read bronze data
        df_bronze = read_bronze_data(BRONZE_FULL_NAME)
        
        # Step 2: Deduplicate based on latest ingestion_timestamp
        df_deduplicated = deduplicate_data(df_bronze)
        
        # Step 3: Select silver layer columns
        df_silver = select_silver_columns(df_deduplicated, SILVER_COLUMNS)
        
        # Step 4: Add processing metadata
        df_final = add_processing_metadata(df_silver)
        
        # Step 5: Validate data quality
        is_valid = validate_silver_data(df_final)
        
        if not is_valid:
            raise ValueError("Silver layer validation failed - check data quality issues above")
        
        # Step 6: Write to silver table
        write_to_silver_table(df_final, SILVER_FULL_NAME, write_mode='overwrite')
        
        print("\n" + "="*60)
        print("✓ SILVER LAYER TRANSFORMATION - COMPLETED SUCCESSFULLY")
        print("="*60 + "\n")
        
    except Exception as e:
        print("\n" + "="*60)
        print(f"✗ SILVER LAYER TRANSFORMATION - FAILED")
        print(f"Error: {str(e)}")
        print("="*60 + "\n")
        raise


if __name__ == "__main__":
    main()
