from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime, time, timedelta

# Define Version
class Version_Info(BaseModel):
	
	# Device Hardware Version
	Hardware: Optional[str] = None
	
	# Device Firmware Version
	Firmware: Optional[str] = None
