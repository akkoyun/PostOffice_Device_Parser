# Import Libraries
import logging, coloredlogs

# Set Log Options
logging.basicConfig(filename='Log/Service.LOG', level=logging.INFO)
Service_Logger = logging.getLogger(__name__)

# Set Log Colored
coloredlogs.install(level='DEBUG', logger=Service_Logger)

# Boot Log
def Service_Start():
	Service_Logger.info("API Log --> Service Started.")

# Database Log
def Database_Connect():
	Service_Logger.debug("API Log --> Connected to Database")
def Database_DisConnect():
	Service_Logger.debug("API Log --> Connection Closed")

# LOG Kafka Header
def Kafka_Header(Command, Device_ID, Device_IP, Device_Time, Kafka_Topic, Kafka_Partition, Kafka_Offset):

	# Print LOG
	Service_Logger.debug(f"Command     : '{Command}'")
	Service_Logger.debug(f"Device ID   : '{Device_ID}'")
	Service_Logger.debug(f"Client IP   : '{Device_IP}'")
	Service_Logger.debug(f"Device Time : '{Device_Time}'")


#	print("................................................................................")
#	print("Command     : ", Command)
#	print("Device ID   : ", Device_ID)
#	print("Client IP   : ", Device_IP)
#	print("Device Time : ", Device_Time)
#	print("Topic : ", Kafka_Topic, " - Partition : ", Kafka_Partition, " - Offset : ", Kafka_Offset)
#	print("................................................................................")
