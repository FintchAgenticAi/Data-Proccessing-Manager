import os

os.environ["DATABASE_BACKEND"] = "sqlite"
os.environ.pop("DATABASE_URL", None)
