import os

# Tests never touch the dev database.
os.environ.setdefault("MONGO_DB", "credit_service_test")
# ...nor the dev queue, so a running worker container cannot steal test messages.
os.environ.setdefault("CREDITS_QUEUE", "credits_test")
