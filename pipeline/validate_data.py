"""
Data quality validation for Smart City pipeline (pandas + Great Expectations).
Validates weather and traffic DataFrames before they are saved as Parquet to Silver.
"""
import great_expectations as gx



from pipeline.utils.connect import get_logger

logger = get_logger("validate_data")


def build_weather_suite() -> gx.ExpectationSuite:
    """Expectations mirroring the old Spark validate_data rules for weather."""
    suite = gx.ExpectationSuite(name="weather_suite")

    critical_columns = ["ingested_at", "timestamp", "temp", "humidity", "weather_condition"]
    for column in critical_columns:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
        )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="temp", min_value=-2, max_value=48)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="ingested_at")
    )
    return suite


def build_traffic_suite() -> gx.ExpectationSuite:
    """Expectations mirroring the old Spark validate_data rules for traffic."""
    suite = gx.ExpectationSuite(name="traffic_suite")

    critical_columns = ["ingested_at", "currentSpeed", "confidence"]
    for column in critical_columns:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
        )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="currentSpeed", min_value=0.0, max_value=130.0)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="confidence", min_value=0.5, max_value=1.0)
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="ingested_at")
    )
    return suite


def run_validation(df: "pandas.DataFrame", source: str) -> bool:  # type: ignore
    """
    Run all validations for a given DataFrame using Great Expectations.
    Logs results and returns overall pass/fail — same contract as before.

    Args:
        df: pandas DataFrame to validate.
        source: 'weather' or 'traffic'.

    Returns:
        True if all expectations pass, False if any fail.
    """
    logger.info(f"Starting validation for source: {source}")
    # Ephemeral context: nothing persisted to disk, rebuilt fresh each call.
    context = gx.get_context(mode="ephemeral")

    if source == "weather":
        suite = build_weather_suite()
    elif source == "traffic":
        suite = build_traffic_suite()
    else:
        logger.warning(f"⚠️ Unknown Source '{source}'")
        return False

    # Fine for our volume — no need for a full GX project (file-backed context).

    data_source = context.data_sources.add_pandas(f"{source}_datasource")
    data_asset = data_source.add_dataframe_asset(name=f"{source}_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{source}_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    result = batch.validate(suite)

    # Fail-slow: log every failed expectation, not just the first one.
    for expectation_result in result.results:
        if not expectation_result.success:
            kwargs = expectation_result.expectation_config.kwargs
            column = kwargs.get("column")
            exp_type = expectation_result.expectation_config.type
            unexpected_count = expectation_result.result.get("unexpected_count", "?")
            logger.warning(
                f"⚠️ Validation failed: '{exp_type}' on column '{column}' "
                f"— {unexpected_count} unexpected value(s)"
            )

    if result.success:
        logger.info(f"✅ Validation passed for {source}")
    else:
        logger.warning(f"⚠️ Validation failed for {source}")

    return result.success