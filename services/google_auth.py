import os
import pathlib

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
CLIENT_SECRETS_FILE = os.path.join(PROJECT_ROOT, "client_secret.json")

REDIRECT_URI = "http://localhost:8000/auth/callback"
SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
]


def get_google_auth_flow():
    """Initializes and returns the Google OAuth Flow."""
    flow = Flow.from_client_secrets_file(client_secrets_file=CLIENT_SECRETS_FILE, scopes=SCOPES, redirect_uri=REDIRECT_URI)
    return flow


def get_user_info(credentials: Credentials):
    """Fetches user info using their credentials."""
    service = build("oauth2", "v2", credentials=credentials)
    user_info = service.userinfo().get().execute()
    return user_info
