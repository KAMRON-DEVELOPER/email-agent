from enum import Enum, auto
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import TIMESTAMP
from sqlalchemy import UUID as PG_UUID
from sqlalchemy import Enum as PGEnum
from sqlalchemy import ForeignKey, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class AutoName(Enum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values):
        return name


# --------------------------
# Enums
# --------------------------
class OrderStatus(AutoName):
    pending = auto()
    refund_requested = auto()
    refunded = auto()


class RefundStatus(AutoName):
    pending = auto()
    processed = auto()
    not_found = auto()


class EmailCategory(AutoName):
    question = auto()
    refund = auto()
    other = auto()


class ImportanceLevel(AutoName):
    high = auto()
    medium = auto()
    low = auto()


# --------------------------
# Base
# --------------------------
class Base(DeclarativeBase):
    pass


class BaseModel(Base):
    __abstract__ = True
    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), default=uuid4, primary_key=True)
    created_at: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP(timezone=True), default=func.now())
    updated_at: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP(timezone=True), default=func.now(), onupdate=func.now())


# --------------------------
# AccountModel
# --------------------------
class AccountModel(BaseModel):
    __tablename__ = "account_table"

    session_id: Mapped[str] = mapped_column(String(255), nullable=False)
    gmail_address: Mapped[str] = mapped_column(String(255), nullable=False)
    credentials: Mapped[str] = mapped_column(Text, nullable=False)

    emails: Mapped[list["EmailModel"]] = relationship(back_populates="account", cascade="all, delete-orphan")


# --------------------------
# OrderModel
# --------------------------
class OrderModel(BaseModel):
    __tablename__ = "order_table"

    customer_email: Mapped[str] = mapped_column(String(255), nullable=False)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    order_status: Mapped[OrderStatus] = mapped_column(PGEnum(OrderStatus, name="order_status_enum"), default=OrderStatus.pending)

    refund_requests: Mapped[list["RefundRequestModel"]] = relationship(back_populates="order", cascade="all, delete-orphan")


# --------------------------
# EmailModel
# --------------------------
class EmailModel(BaseModel):
    __tablename__ = "email_table"

    message_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    thread_id: Mapped[str] = mapped_column(String(255), nullable=False)
    subject: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    from_email: Mapped[str] = mapped_column(String(255), nullable=False)
    to_email: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[EmailCategory] = mapped_column(PGEnum(EmailCategory, name="email_category_enum"), nullable=False)

    account_id: Mapped[Optional[UUID]] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("account_table.id"), nullable=True)
    account: Mapped[Optional["AccountModel"]] = relationship(back_populates="emails")

    refund_requests: Mapped[list["RefundRequestModel"]] = relationship(back_populates="email", cascade="all, delete-orphan")
    unhandled_info: Mapped[Optional["UnhandledEmailModel"]] = relationship(back_populates="email", uselist=False, cascade="all, delete-orphan")


# --------------------------
# RefundRequestModel
# --------------------------
class RefundRequestModel(BaseModel):
    __tablename__ = "refund_request_table"

    email_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("email_table.id"), nullable=False)
    order_id: Mapped[Optional[UUID]] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("order_table.id"), nullable=True)
    status: Mapped[RefundStatus] = mapped_column(PGEnum(RefundStatus, name="refund_status_enum"), default=RefundStatus.pending)
    provided_order_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)  # store invalid IDs too

    email: Mapped["EmailModel"] = relationship(back_populates="refund_requests")
    order: Mapped[Optional["OrderModel"]] = relationship(back_populates="refund_requests")


# --------------------------
# UnhandledEmailModel
# --------------------------
class UnhandledEmailModel(BaseModel):
    __tablename__ = "unhandled_email_table"

    email_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), ForeignKey("email_table.id"), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    importance: Mapped[ImportanceLevel] = mapped_column(PGEnum(ImportanceLevel, name="importance_level_enum"), nullable=False)

    email: Mapped["EmailModel"] = relationship(back_populates="unhandled_info")
