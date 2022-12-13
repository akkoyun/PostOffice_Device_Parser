# Import Libraries
import logging

# Set Log Options
logging.basicConfig(filename='Log/Service.LOG', level=logging.INFO)
Service_Logger = logging.getLogger(__name__)


# Database Log
def LOG_Database_Connect():
	Service_Logger.info(f"API Log --> Connected to Database")
def LOG_Database_DisConnect():
	Service_Logger.info(f"API Log --> Connection Closed")










# LOG Kafka Header
def Log_Kafka_Header(Command, Device_ID, Device_IP, Device_Time, Kafka_Topic, Kafka_Partition, Kafka_Offset):

	# Print LOG
	print("................................................................................")
	print("Command     : ", Command)
	print("Device ID   : ", Device_ID)
	print("Client IP   : ", Device_IP)
	print("Device Time : ", Device_Time)
	print("Topic : ", Kafka_Topic, " - Partition : ", Kafka_Partition, " - Offset : ", Kafka_Offset)
	print("................................................................................")
