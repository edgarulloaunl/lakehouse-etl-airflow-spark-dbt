import great_expectations as gx

context = gx.get_context()

datasource = context.get_datasource("postgres_warehouse")
asset = datasource.get_asset("raw_transactions")

batch_request = asset.build_batch_request()

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="transactions_suite"
)

validator.expect_column_values_to_not_be_null("user_id")
validator.expect_column_values_to_be_between("amount", min_value=0)

validator.save_expectation_suite()

print("Expectations creadas")