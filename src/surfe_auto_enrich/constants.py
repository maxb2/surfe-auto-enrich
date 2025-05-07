import os

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

API_URL = "https://api.surfe.com/v2/people/enrich"

API_HEADERS = {"Authorization": f"Bearer {API_KEY}"}
