import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
INTERIM_DATA_DIR = os.path.join(DATA_DIR, "interim")

MODELS_DIR = os.path.join(BASE_DIR, "models")

NOTEBOOKS_DIR = os.path.join(BASE_DIR, "notebooks")

REPORTS_DIR = os.path.join(BASE_DIR, "reports")

