import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "ai_dw_superset_secret_change_in_prod")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Allow embedding in iframe (for integration scenarios)
WTF_CSRF_ENABLED = False
SESSION_COOKIE_SAMESITE = "Lax"

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,  # allow Jinja2 in SQL
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

# Row limit for SQL Lab
SQL_MAX_ROW = 100000
DISPLAY_MAX_ROW = 10000
