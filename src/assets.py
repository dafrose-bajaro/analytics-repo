from dagster import asset


@asset
def hello_world():
    """A simple asset that returns a greeting."""
    return "Hello, World!"
