import sentry_sdk
sentry_sdk.init(
    dsn="http://93f853fc7fb24e0a9511b4357af052f7@localhost:9010/2",
    debug=True,
    shutdown_timeout=10
)
sentry_sdk.capture_message("Sentry is successfully connected from Spark job!")
sentry_sdk.flush()
