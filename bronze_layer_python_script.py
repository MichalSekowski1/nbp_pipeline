"""
Bronze Layer: NBP Exchange Rates Ingestion
Fetches exchange rate data from NBP API and writes to Bronze Delta table
"""

from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import time
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, current_timestamp, col, count, when, isnan, isnull, to_date, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NBP Bronze Layer Ingestion") \
    .getOrCreate()

# Bronze layer table configuration
CATALOG = 'workspace'
SCHEMA = 'bronze'
BRONZE_TABLE = 'nbp_exchange_rates_bronze'
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{BRONZE_TABLE}"

# Configuration parameters
YEARS_BACK = 5
CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY']
API_BASE_URL = 'https://api.nbp.pl/api/exchangerates/rates/a'

# Data quality thresholds
QUALITY_THRESHOLDS = {
    'max_null_percentage': 0.0,  # No nulls allowed in critical fields
    'max_duplicate_percentage': 0.0,  # No duplicates allowed
    'min_expected_records': 100,  # Minimum records per currency
    'strict_mode': False  # Set to True to fail job on quality issues
}

print(f"Target table: {FULL_TABLE_NAME}")
print(f"Spark version: {spark.version}")


def fetch_exchange_rates(currency_code, start_date, end_date, max_retries=3):
    """
    Fetch exchange rates for a single currency within a date range.
    
    Args:
        currency_code: Currency code (e.g., 'USD', 'EUR')
        start_date: Start date as datetime object
        end_date: End date as datetime object
        max_retries: Maximum number of retry attempts
    
    Returns:
        Dictionary with API response or None if failed
    """
    url = f"{API_BASE_URL}/{currency_code.lower()}/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}/?format=json"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✓ {currency_code}: Fetched {len(data.get('rates', []))} records from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                return data
            
            elif response.status_code == 404:
                print(f"⚠ {currency_code}: No data found for period {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                return None
            
            elif response.status_code == 400:
                error_msg = response.text
                if 'Przekroczony limit' in error_msg or 'limit' in error_msg.lower():
                    print(f"✗ {currency_code}: Period too large (exceeds 367 days limit)")
                else:
                    print(f"✗ {currency_code}: Bad request - {error_msg}")
                return None
            
            else:
                print(f"✗ {currency_code}: Unexpected status {response.status_code} - {response.text}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
        
        except requests.exceptions.Timeout:
            print(f"⚠ {currency_code}: Request timeout (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
        
        except requests.exceptions.RequestException as e:
            print(f"✗ {currency_code}: Request failed - {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None
    
    return None


def generate_yearly_chunks(start_date, end_date):
    """
    Split date range into yearly chunks to respect API 367-day limit.
    """
    chunks = []
    current_start = start_date
    
    while current_start < end_date:
        current_end = min(current_start + relativedelta(years=1), end_date)
        chunks.append((current_start, current_end))
        current_start = current_end + relativedelta(days=1)
    
    return chunks


def collect_exchange_rate_data(currencies, start_date, end_date):
    """
    Collect exchange rate data for all currencies within date range.
    
    Returns:
        List of dictionaries with exchange rate records
    """
    time_chunks = generate_yearly_chunks(start_date, end_date)
    print(f"\nSplit into {len(time_chunks)} yearly chunks:")
    for i, (chunk_start, chunk_end) in enumerate(time_chunks, 1):
        print(f"  Chunk {i}: {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
    
    all_data = []
    
    print(f"\n{'='*60}")
    print("Starting data collection...")
    print(f"{'='*60}\n")
    
    for currency in currencies:
        print(f"Processing {currency}...")
        currency_data = []
        
        for chunk_start, chunk_end in time_chunks:
            result = fetch_exchange_rates(currency, chunk_start, chunk_end)
            
            if result and 'rates' in result:
                for rate in result['rates']:
                    # Flatten the structure for Bronze layer
                    record = {
                        'currency_code': result['code'],
                        'currency_name': result['currency'],
                        'table': result['table'],
                        'no': rate['no'],
                        'effectiveDate': rate['effectiveDate'],
                        'mid': rate['mid']
                    }
                    currency_data.append(record)
            
            time.sleep(0.5)  # Be respectful to the API
        
        all_data.extend(currency_data)
        print(f"  Total records for {currency}: {len(currency_data)}\n")
    
    print(f"{'='*60}")
    print(f"Collection complete! Total records: {len(all_data)}")
    print(f"{'='*60}\n")
    
    return all_data


def validate_data_quality(df, thresholds):
    """
    Perform comprehensive data quality checks on the DataFrame.
    
    Args:
        df: PySpark DataFrame to validate
        thresholds: Dictionary with quality thresholds
    
    Returns:
        Tuple of (is_valid, quality_report)
    """
    print("\n" + "="*60)
    print("DATA QUALITY VALIDATION")
    print("="*60 + "\n")
    
    total_records = df.count()
    quality_report = {
        'total_records': total_records,
        'checks': [],
        'warnings': [],
        'errors': []
    }
    
    # Check 1: Null value detection in critical columns
    print("Check 1: Null Value Detection")
    critical_columns = ['currency_code', 'effectiveDate', 'mid']
    null_checks = {}
    
    for col_name in critical_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_percentage = (null_count / total_records * 100) if total_records > 0 else 0
        null_checks[col_name] = {'count': null_count, 'percentage': null_percentage}
        
        status = "✓" if null_count == 0 else "✗"
        print(f"  {status} {col_name}: {null_count} nulls ({null_percentage:.2f}%)")
        
        if null_percentage > thresholds['max_null_percentage']:
            quality_report['errors'].append(f"Column '{col_name}' has {null_percentage:.2f}% nulls (threshold: {thresholds['max_null_percentage']}%)")
    
    quality_report['checks'].append({'name': 'null_check', 'results': null_checks})
    
    # Check 2: Duplicate detection (currency_code + effectiveDate should be unique)
    print("\nCheck 2: Duplicate Detection")
    duplicate_count = df.groupBy('currency_code', 'effectiveDate').count().filter(col('count') > 1).count()
    duplicate_percentage = (duplicate_count / total_records * 100) if total_records > 0 else 0
    
    status = "✓" if duplicate_count == 0 else "⚠"
    print(f"  {status} Found {duplicate_count} duplicate combinations ({duplicate_percentage:.2f}%)")
    
    quality_report['checks'].append({
        'name': 'duplicate_check',
        'duplicate_count': duplicate_count,
        'duplicate_percentage': duplicate_percentage
    })
    
    if duplicate_percentage > thresholds['max_duplicate_percentage']:
        quality_report['warnings'].append(f"Found {duplicate_percentage:.2f}% duplicates (threshold: {thresholds['max_duplicate_percentage']}%)")
    
    # Check 3: Exchange rate value validation (must be positive)
    print("\nCheck 3: Exchange Rate Value Validation")
    invalid_rates = df.filter((col('mid') <= 0) | isnan(col('mid'))).count()
    invalid_percentage = (invalid_rates / total_records * 100) if total_records > 0 else 0
    
    status = "✓" if invalid_rates == 0 else "✗"
    print(f"  {status} Invalid rates (<=0 or NaN): {invalid_rates} ({invalid_percentage:.2f}%)")
    
    if invalid_rates > 0:
        quality_report['errors'].append(f"Found {invalid_rates} invalid exchange rates")
    
    # Get rate statistics
    rate_stats = df.select('mid').summary('min', 'max', 'mean').collect()
    print(f"  Rate range: {rate_stats[0]['mid']} to {rate_stats[1]['mid']}")
    print(f"  Mean rate: {float(rate_stats[2]['mid']):.4f}")
    
    quality_report['checks'].append({
        'name': 'value_validation',
        'invalid_count': invalid_rates,
        'min': rate_stats[0]['mid'],
        'max': rate_stats[1]['mid'],
        'mean': rate_stats[2]['mid']
    })
    
    # Check 4: Date format and range validation
    print("\nCheck 4: Date Format and Range Validation")
    df_with_date = df.withColumn('parsed_date', to_date(col('effectiveDate'), 'yyyy-MM-dd'))
    invalid_dates = df_with_date.filter(col('parsed_date').isNull()).count()
    
    status = "✓" if invalid_dates == 0 else "✗"
    print(f"  {status} Invalid date formats: {invalid_dates}")
    
    if invalid_dates > 0:
        quality_report['errors'].append(f"Found {invalid_dates} records with invalid date format")
    
    # Check date range
    date_range = df_with_date.select(
        spark_min('parsed_date').alias('min_date'),
        spark_max('parsed_date').alias('max_date')
    ).collect()[0]
    print(f"  Date range: {date_range['min_date']} to {date_range['max_date']}")
    
    quality_report['checks'].append({
        'name': 'date_validation',
        'invalid_dates': invalid_dates,
        'min_date': str(date_range['min_date']),
        'max_date': str(date_range['max_date'])
    })
    
    # Check 5: Record count by currency
    print("\nCheck 5: Record Count by Currency")
    currency_counts = df.groupBy('currency_code').count().orderBy('currency_code').collect()
    
    for row in currency_counts:
        currency = row['currency_code']
        record_count = row['count']
        status = "✓" if record_count >= thresholds['min_expected_records'] else "⚠"
        print(f"  {status} {currency}: {record_count} records")
        
        if record_count < thresholds['min_expected_records']:
            quality_report['warnings'].append(f"Currency {currency} has only {record_count} records (expected >= {thresholds['min_expected_records']})")
    
    quality_report['checks'].append({
        'name': 'record_count_by_currency',
        'counts': {row['currency_code']: row['count'] for row in currency_counts}
    })
    
    # Check 6: Data completeness check
    print("\nCheck 6: Data Completeness")
    expected_currencies = set(CURRENCIES)
    actual_currencies = set([row['currency_code'] for row in currency_counts])
    missing_currencies = expected_currencies - actual_currencies
    
    if missing_currencies:
        status = "⚠"
        print(f"  {status} Missing currencies: {', '.join(missing_currencies)}")
        quality_report['warnings'].append(f"Missing data for currencies: {', '.join(missing_currencies)}")
    else:
        status = "✓"
        print(f"  {status} All expected currencies present")
    
    # Summary
    print("\n" + "="*60)
    print("QUALITY VALIDATION SUMMARY")
    print("="*60)
    print(f"Total Records: {total_records}")
    print(f"Errors: {len(quality_report['errors'])}")
    print(f"Warnings: {len(quality_report['warnings'])}")
    
    if quality_report['errors']:
        print("\nErrors:")
        for error in quality_report['errors']:
            print(f"  ✗ {error}")
    
    if quality_report['warnings']:
        print("\nWarnings:")
        for warning in quality_report['warnings']:
            print(f"  ⚠ {warning}")
    
    # Determine if data passes validation
    is_valid = len(quality_report['errors']) == 0
    
    if is_valid:
        print("\n✓ Data quality validation PASSED")
    else:
        print("\n✗ Data quality validation FAILED")
    
    print("="*60 + "\n")
    
    return is_valid, quality_report


def write_to_bronze_table(data, table_name, write_mode='append', run_quality_checks=True):
    """
    Write collected data to Bronze Delta table with optional quality checks.
    
    Args:
        data: List of dictionaries with exchange rate records
        table_name: Fully qualified table name
        write_mode: 'append' or 'overwrite'
        run_quality_checks: Whether to run quality validation before writing
    """
    if not data:
        print("⚠ No data to write!")
        return
    
    print(f"Creating DataFrame from {len(data)} records...")
    
    # Define schema for consistency
    schema = StructType([
        StructField("currency_code", StringType(), False),
        StructField("currency_name", StringType(), False),
        StructField("table", StringType(), False),
        StructField("no", StringType(), False),
        StructField("effectiveDate", StringType(), False),
        StructField("mid", DoubleType(), False)
    ])
    
    # Convert list of dicts to DataFrame
    df = spark.createDataFrame([Row(**record) for record in data], schema=schema)
    
    # Add ingestion metadata
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    print(f"DataFrame created with {df.count()} rows")
    print("\nSchema:")
    df.printSchema()
    
    print(f"\nSample data (first 5 rows):")
    df.show(5, truncate=False)
    
    # Run quality checks if enabled
    if run_quality_checks:
        is_valid, quality_report = validate_data_quality(df, QUALITY_THRESHOLDS)
        
        if not is_valid and QUALITY_THRESHOLDS['strict_mode']:
            error_msg = f"Data quality validation failed in strict mode. Errors: {quality_report['errors']}"
            print(f"\n✗ {error_msg}")
            raise ValueError(error_msg)
        elif not is_valid:
            print("\n⚠ Data quality issues detected, but continuing (strict_mode=False)")
    
    # Write to Delta table
    print(f"\nWriting to {table_name} (mode: {write_mode})...")
    
    df.write \
        .format("delta") \
        .mode(write_mode) \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
    
    print(f"✓ Successfully wrote {df.count()} records to {table_name}")
    
    # Verify write
    result_count = spark.table(table_name).count()
    print(f"✓ Table now contains {result_count} total records")


def main():
    """
    Main execution function
    """
    try:
        # Calculate date range
        today = datetime.now()
        start_date = today - relativedelta(years=YEARS_BACK)
        
        print(f"Fetching data from {start_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}")
        print(f"Currencies: {', '.join(CURRENCIES)}")
        
        # Collect data from API
        data = collect_exchange_rate_data(CURRENCIES, start_date, today)
        
        if data:
            # Write to Bronze table with quality checks
            write_to_bronze_table(
                data, 
                FULL_TABLE_NAME, 
                write_mode='append',
                run_quality_checks=True
            )
            print("\n✓ Bronze layer ingestion completed successfully!")
        else:
            print("\n⚠ No data collected - check API status and date ranges")
            
    except Exception as e:
        print(f"\n✗ Error during ingestion: {str(e)}")
        raise


if __name__ == "__main__":
    main()
