from typing import Optional

from pydantic import EmailStr, field_validator, BaseModel


class MessageBody(BaseModel):
    size: int
    data: Optional[str] = None


class MessageHeader(BaseModel):
    name: str
    value: str


class MessagePart(BaseModel):
    partId: str
    mimeType: str
    filename: str
    headers: list[MessageHeader] = []
    body: MessageBody
    parts: Optional[list["MessagePart"]] = None


MessagePart.model_rebuild()


class MessagePayload(BaseModel):
    partId: str
    mimeType: str
    filename: Optional[str] = None
    headers: list[MessageHeader] = []
    body: MessageBody
    parts: Optional[list[MessagePart]] = None


class MessageResponse(BaseModel):
    id: str
    threadId: str
    labelIds: Optional[list[str]] = None
    snippet: Optional[str] = None
    payload: MessagePayload
    sizeEstimate: Optional[int] = None
    historyId: Optional[str] = None
    internalDate: Optional[str] = None


# ------------------------------------

class GmailMessage(BaseModel):
    id: str
    threadId: str
    labelIds: Optional[list[str]] = None


class LabelsRemovedEntry(BaseModel):
    message: GmailMessage
    labelIds: list[str] = []


class HistoryRecord(BaseModel):
    id: str
    messages: Optional[list[GmailMessage]] = None
    labelsRemoved: Optional[list[LabelsRemovedEntry]] = None


class History(BaseModel):
    historyId: str
    history: list[HistoryRecord] = []


class ReceivedMessageData(BaseModel):
    emailAddress: EmailStr
    historyId: int

    @field_validator("historyId")
    def check_length(cls, v):
        if len(str(v)) != 7:
            raise ValueError("historyId must be exactly 7 digits long")
        return v
