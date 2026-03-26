from pydantic.v1 import BaseModel, Field
from sekoia_automation.connector import DefaultConnectorConfiguration


class SaviyntConfiguration(BaseModel):
    username: str = Field(..., description="Username used for API Access")
    password: str = Field(secret=True, description="Password used for API Access")
    base_url: str = Field(..., description="Base Url of the Saviynt platform")

class SaviyntConnectorConfiguration(DefaultConnectorConfiguration, BaseModel):
    analytics_name: list = Field(..., description="List of the analytics you want to monitor")
    frequency: int = Field(..., description="Batch frequency in seconds")