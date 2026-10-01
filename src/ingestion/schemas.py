from typing import Optional
from pydantic import BaseModel, Field


class TransactionEvent(BaseModel):
    transaction_id: str = Field(..., description="Unique UUID identifier")
    account_id: str = Field(..., description="Entity partition key")
    amount: float = Field(..., gt=0, description="Transaction amount in USD")
    timestamp: float = Field(..., description="Unix epoch timestamp in seconds")
    merchant_id: str = Field(..., description="Merchant identifier")
    merchant_category: str = Field(..., description="Merchant category code")
    country: str = Field(..., min_length=2, max_length=2, description="ISO-2 country code")
    is_anomaly: Optional[bool] = Field(False, description="Ground truth audit label")

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")