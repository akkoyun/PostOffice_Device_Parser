# Import Libraries
import logging

# Set Log Options
logging.basicConfig(filename='Log/Service.LOG', level=logging.INFO)
Service_Logger = logging.getLogger(__name__)

# Logger Format Function
class CustomFormatter(logging.Formatter):

	# Define Consts
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

	# Define Formats
    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

# Define Logger
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
Service_Logger.addHandler(ch)

# Boot Log
def Service_Start():
	Service_Logger.info("API Log --> Service Started.")

# Database Log
def Database_Connect():
	Service_Logger.debug("API Log --> Connected to Database")
def Database_DisConnect():
	Service_Logger.debug("API Log --> Connection Closed")










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
