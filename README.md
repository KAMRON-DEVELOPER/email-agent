# Gmail Agent 📧

A FastAPI application that connects Gmail accounts using Google OAuth2 and listens for new email events in real time with Google Cloud Pub/Sub.

---

## Google Cloud Platform (GCP) Setup ☁️

Before running the application, configure your Google Cloud project:

- **Create or Select a Project**: Use the GCP Console to create a new project or select an existing one.
- **Enable APIs**: In *APIs & Services > Library*, enable:
    - Gmail API
    - Google Cloud Pub/Sub API
- **Create OAuth 2.0 Credentials**:
    - Go to *APIs & Services > Credentials*.
    - Click *Create Credentials* → *OAuth 2.0 Client ID*.
    - Set the application type to **Web application**.
    - Add this authorized redirect URI:
      ```
      http://localhost:8000/auth/callback
      ```
    - Download the JSON file and rename it to **client_secret.json**.
- **Create a Service Account**:
    - In *Credentials*, select *Create Credentials* → *Service Account*.
    - Grant it the **Pub/Sub Subscriber** role.
    - Under *Keys*, add a new JSON key, download it, and rename it to **service-account.json**.
- **Configure Pub/Sub**:
    - In *Pub/Sub*, create a topic named `gmail-events`.
    - Create a subscription for it named `gmail-subscriptions`.
    - If you use different names, update them in the project configuration.

---

## Project Setup & Configuration

1. **Place Credentials**  
   Put the two JSON files from the steps above into the project root:
    - `client_secret.json`
    - `service-account.json`

2. **Database Setup**  
   This project uses PostgreSQL. By default, it connects to:
    - Database: `gmail_agent_db`
    - User: `postgres`
    - Password: `password`

---

## Installation & Running

```bash
# Install dependencies
uv sync --dev

# Start the FastAPI server
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Alembic migrations (Optional)
alembic revision --autogenerate -m "initial migration"
alembic upgrade head
