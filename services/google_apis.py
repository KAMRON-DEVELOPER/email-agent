from googleapiclient.discovery import build


def get_new_messages(creds, start_history_id):
    service = build("gmail", "v1", credentials=creds)
    history = service.users().history().list(userId="me", startHistoryId=start_history_id).execute()
    for record in history.get("history", []):
        if "messagesAdded" in record:
            for msg in record["messagesAdded"]:
                email = service.users().messages().get(userId="me", id=msg["message"]["id"]).execute()
                print(email)
