import great_expectations as gx

context = gx.get_context()

suite = context.add_expectation_suite(
    expectation_suite_name="transactions_suite"
)

print("Suite creada")