from sqlalchemy import Column, Integer, String
from sqlalchemy.sql.expression import text
from sqlalchemy.sql.sqltypes import TIMESTAMP
from .Database import Base

# Device Version Table Model
class Version(Base):

	# Define Table Name
	__tablename__ = "Version"

	# Define Colomns
	Version_ID = Column(Integer, primary_key=True, nullable=False)
	Device_ID = Column(String, nullable=False)
	Hardware_Version = Column(String, nullable=False)
	Firmware_Version = Column(String, nullable=False)
	Update_Time = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text('now()'))
