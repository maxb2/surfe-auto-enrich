import os

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

API_URL = "https://api.surfe.com/v2/people/enrich"

API_HEADERS = {"Authorization": f"Bearer {API_KEY}"}

OUTPUT_COLUMNS_SAFE = [
    "externalID",
    "firstName",
    "lastName",
    "email",
    "linkedInUrl",
    "firstName_surfe",
    "lastName_surfe",
    "companyName",
    "companyDomain",
    "linkedInUrl_surfe",
    "jobTitle",
    "country",
    #  'jobHistory', # The field causing issues
    "status",
    "crm_email_in_surfe_results",
    "has_diff",
    "emails_surfe",
    "emails_surfeValidationStatus",
]
