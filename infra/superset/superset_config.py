"""Local Superset configuration for the CĐTN serving profile."""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_METADATA_DATABASE_URI"]

# Local single-process serving profile. Redis/Celery are intentionally out of scope.
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_THRESHOLD": 1000,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
RESULTS_BACKEND = None

ROW_LIMIT = 50000
SQL_MAX_ROW = 50000
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = False

# The Trino driver is installed in the custom image; connection is created in the UI.
