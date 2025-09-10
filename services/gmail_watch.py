from google.auth.credentials import Credentials
from googleapiclient.discovery import build

from utils.logger import logger

PROJECT_ID = "gmail-api-project-471605"
TOPIC_ID = "gmail-events"
TOPIC_NAME = "projects/{PROJECT_ID}/topics/{TOPIC_ID}".format(PROJECT_ID=PROJECT_ID, TOPIC_ID=TOPIC_ID)


def start_watching_gmail(credentials: Credentials):
    gmail_service = build("gmail", "v1", credentials=credentials)
    request = {"labelIds": ["INBOX"], "topicName": TOPIC_NAME}
    response = gmail_service.users().watch(userId="me", body=request).execute()
    logger.debug("Watch response:", response)
    return response["historyId"]
