import os
import sentry_sdk

def initialize_sentry():
    """

    """
    with open("/etc/spark/sentry_dsn.txt", "r") as file:
        dsn = file.read().strip()

    if not dsn:
        raise ValueError("SENTRY_DSN variable is not set or is empty.")
    
    # Initialize Sentry with the DSN from the environment variable
    sentry_sdk.init(
        dsn=dsn,
        debug=True,
        shutdown_timeout=10
    )
    print("Sentry successfully initialized with DSN:", dsn)

try:
    # Initialize Sentry
    initialize_sentry()
    
    # Send a manual test message to Sentry
    sentry_sdk.capture_message("Sentry is successfully connected from Spark job!")
    sentry_sdk.flush()
    
    # Simulate an error
    raise ValueError("This is a test error to trigger Sentry reporting.")
    
except Exception as e:
    # Capture any exceptions that occur
    print("An error occurred:", e)
    sentry_sdk.capture_exception(e)
    sentry_sdk.flush()
    
finally:
    # Ensure Sentry flushes any remaining events
    sentry_sdk.flush()
