"""
Sentry Integration for Error Reporting

This script initializes the Sentry SDK with a provided DSN, sends a test message to verify the connection,
and simulates an error to demonstrate exception reporting to Sentry.
"""

import os
import sentry_sdk

# Function to initialize Sentry with DSN from environment variable
def initialize_sentry():
    """
    Initialize Sentry with the provided DSN.

    Raises:
        ValueError: If the DSN is not set or is empty.
    """
    dsn = "http://11256d3150814566967391c9272ca734@localhost:9010/1"  # Replace with actual DSN

    # Check if DSN is provided
    if not dsn:
        raise ValueError("SENTRY_DSN environment variable is not set or is empty.")
    
    # Initialize Sentry with the provided DSN
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
    sentry_sdk.flush()  # Ensure the message is sent immediately
    
    # Simulate an error to trigger Sentry reporting
    raise ValueError("This is a test error to trigger Sentry reporting.")
    
except Exception as e:
    # Capture any exceptions that occur
    print("An error occurred:", e)
    sentry_sdk.capture_exception(e)  # Report the exception to Sentry
    sentry_sdk.flush()  # Ensure the exception is sent immediately
    
finally:
    # Ensure Sentry flushes any remaining events before exiting
    sentry_sdk.flush()
