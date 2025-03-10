import os

from dotenv import load_dotenv
from pathlib import Path


BASE_DIR = Path(__name__).absolute().parent
load_dotenv(BASE_DIR / ".env")

# ghost
GHOST_ADMIN_API_KEY = os.getenv('GHOST_ADMIN_API_KEY')
GHOST_API_URL = ""
GHOST_ADMIN_URL = f"{GHOST_API_URL}/ghost/api/admin/"
MEMBER_LIMIT = 0
# google
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(BASE_DIR / "auth" / "google_api_credentials.json")
PROJECT_ID = os.getenv('PROJECT_ID')
DATASET_ID = ""
TABLE_ID = ""
LOCATION = ""