# Import Libraries
import logging, coloredlogs

# Set Log Options
Service_Logger = logging.getLogger(__name__)
logging.basicConfig(filename='Log/Service.LOG', level=logging.INFO, format='%(asctime)s - %(message)s')

# Set Log Colored
coloredlogs.install(level='DEBUG', logger=Service_Logger)

# Boot Log
def Service_Start():
	Service_Logger.info("API Log --> Service Started.")
def Line():
	Service_Logger.debug("--------------------------------------------------------------------------------")

# Database Log
def Database_Connect():
	Service_Logger.debug("API Log --> Connected to Database")
def Database_DisConnect():
	Service_Logger.debug("API Log --> Connection Closed")

# LOG Kafka Header
def Kafka_Header(Command, Device_ID, Device_IP, Device_Time, Kafka_Topic, Kafka_Partition, Kafka_Offset):
	Service_Logger.debug(f"Command     : '{Command}'")
	Service_Logger.debug(f"Device ID   : '{Device_ID}'")
	Service_Logger.debug(f"Client IP   : '{Device_IP}'")
	Service_Logger.debug(f"Device Time : '{Device_Time}'")
