# Import Libraries
from pydantic import BaseSettings

# Define Setting
class Settings(BaseSettings):

	POSTOFFICE_DB_HOSTNAME: str
	POSTOFFICE_DB_PORT: str
	POSTOFFICE_DB_PASSWORD: str
	POSTOFFICE_DB_NAME: str
	POSTOFFICE_DB_USERNAME: str

	class Config:
		env_file = "config/.env"

# Set Setting
APP_Settings = Settings()
