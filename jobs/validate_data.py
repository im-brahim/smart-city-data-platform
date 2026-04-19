"""
Data quality validation for Smart City pipeline.
Validates weather and traffic DataFrames before
they are saved to PostgreSQL.
"""
# Standard library
import logging

# Third party
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


def validate_no_nulls(df: DataFrame, critical_columns: list) -> bool:
    """
    Check that critical columns contain no null values.

    Args:
        df: Spark DataFrame to validate.
        critical_columns: List of column names that must not be null.

    Returns:
        True if no nulls found, False otherwise.
    """
    is_valid = True
    for column in critical_columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            logger.warning(f"⚠️ Validation failed: Column '{column}' contains {null_count} null values.")
            is_valid = False     # mark as failed but continue checking
    return is_valid              # check ALL columns before returning


def validate_realistic_values(df: DataFrame, column: str, min_val: float, max_val: float) -> bool:
    """
    Check that a numeric column contains only realistic values.

    Args:
        df: Spark DataFrame to validate.
        column: Column name to check.
        min_val: Minimum acceptable value.
        max_val: Maximum acceptable value.

    Returns:
        True if all values are within range, False otherwise.
    """
    is_valid = True        
    invalid_value = df.filter( (col(column) > max_val) | (col(column) < min_val)).count()
    if invalid_value > 0 :
        logger.warning(f" ⚠️ Validation failed: Column '{column}' contains '{invalid_value}' unrealistic values")
        is_valid = False         
    return is_valid


def validate_no_duplicates(df: DataFrame, key_column: str) -> bool:
    """
    Check that a key column contains no duplicate values.

    Args:
        df: Spark DataFrame to validate.
        key_column: Column name that should be unique.

    Returns:
        True if no duplicates found in the Key_Column, False otherwise.
    """
    
    is_valid = True
    total = df.count()
    unique = df.dropDuplicates([key_column]).count()
    duplicated_count = total - unique

    if  duplicated_count > 0 :
        logger.warning(f"⚠️ The Column '{key_column}' contains duplicated values")
        is_valid = False

    return is_valid


def run_validation(df: DataFrame, source: str) -> bool:
    """
    Run all validations for a given DataFrame.
    Logs results and returns overall pass/fail.

    Args:
        df: Spark DataFrame to validate.
        source: Name of data source e.g. 'weather' or 'traffic'

    Returns:
        True if all validations pass, False if any fail.
    """

    logger.info(f"Starting validation for source: {source}")
    is_valid = True

    # Step 1 — define rules based on source
    if source == "weather":
        critical_columns = ['ingested_at', 'timestamp', 'temp', 'humidity', 'weather_condition']         #'main.temp', 'main.humidity']
        min_val = -2
        max_val = 48

    elif source == "traffic":
        critical_columns = ['ingested_at',
                            'currentSpeed',         # 'flowSegmentData.currentSpeed',
                            'confidence'            # 'flowSegmentData.confidence'
                        ]
        min_speed = 0.0
        max_speed = 130.0
        min_confidence = 0.5
        max_confidence = 1.0
    else:
        logger.warning(f" ⚠️ Unknown Source '{source}'")
        return False   


    # Step 2 — check nulls
    logger.info("---------- Checking Nulls... ------------------ ")
    if not validate_no_nulls(df, critical_columns):
        is_valid = False
    else:
        logger.info("✅ No Nulls is Validate")

    # Step 3 — check duplicates
    logger.info("---------- Checking Duplicate... ------------------ ")
    if not validate_no_duplicates(df, 'ingested_at'):
        is_valid = False
    else:
        logger.info("✅ No Duplicate is Validate")
    # Step 4 — check realistic values
    
    if source == "weather":
        # ------------------- WEATHER ------------------------

        logger.info("---------- Checking Realistic Values in WEATHER for TEMPERATURE ... ------------------ ")
        if not validate_realistic_values(df, 'temp',  min_val , max_val):
            is_valid = False 
    else:
        # ------------------- Traffic ------------------------
        
        logger.info("---------- Checking Realistic Values in TRAFFIC for CURRENTSPEED ... ------------------ ")
        if not validate_realistic_values(df, 'currentSpeed',  min_speed , max_speed):
            is_valid = False
        
        logger.info("---------- Checking Realistic Values in TRAFFIC for CURRENTSPEED ... ------------------ ")
        if not validate_realistic_values(df, 'confidence',  min_confidence , max_confidence):
            is_valid = False
    
    if is_valid:
        logger.info(f" ✅ Validation passed for {source}")
    else:
        logger.warning(f"⚠️ Validation failed for {source}")

    return is_valid