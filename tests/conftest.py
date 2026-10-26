import os

# Tests never touch the dev database.
os.environ.setdefault("MONGO_DB", "credit_service_test")
