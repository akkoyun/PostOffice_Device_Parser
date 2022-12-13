# Import Libraries
from Setup import LOG, Database, Schema, Models
from Setup.Config import APP_Settings
from kafka import KafkaConsumer
from sqlalchemy import inspect
import json

# Create DB Models
Database.Base.metadata.create_all(bind=Database.DB_Engine)

# Kafka Consumer
Kafka_Consumer = KafkaConsumer('Device', bootstrap_servers=f"{APP_Settings.POSTOFFICE_KAFKA_HOSTNAME}:{APP_Settings.POSTOFFICE_KAFKA_PORT}", group_id="Data_Consumer", auto_offset_reset='earliest', enable_auto_commit=False)

# Boot Log Message
LOG.Service_Start()

def object_as_dict(obj):
    return {c.key: getattr(obj, c.key)
            for c in inspect(obj).mapper.column_attrs}

# Parser Function
def Device_Parser():

	try:

		for Message in Kafka_Consumer:

			# handle Message.
			Kafka_Message = Schema.IoT_Data_Pack_Device(**json.loads(Message.value.decode()))

			# Handle Headers
			Command = Message.headers[0][1].decode('ASCII')
			Device_ID = Message.headers[1][1].decode('ASCII')
			Device_Time = Message.headers[2][1].decode('ASCII')
			Device_IP = Message.headers[3][1].decode('ASCII')

			# Print LOG
			LOG.Service_Logger.debug("--------------------------------------------------------------------------------")
			LOG.Kafka_Header(Command, Device_ID, Device_IP, Device_Time, Message.topic, Message.partition, Message.offset)
			LOG.Service_Logger.debug("--------------------------------------------------------------------------------")

			# Declare Variables
			class DB_Variables:
				Module_ID = 0		# Module ID 












			# Define DB
			DB_Module = Database.SessionLocal()

			# Database Query
			Query_Module = DB_Module.query(Models.Module).filter(Models.Module.Device_ID.like(Device_ID)).first()

			print(object_as_dict(Query_Module))

			# Handle Record
			if not Query_Module:

				# Create Add Record Command
				New_Module = Models.Module(Device_ID = Device_ID)

				# Add and Refresh DataBase
				DB_Module.add(New_Module)
				DB_Module.commit()
				DB_Module.refresh(New_Module)

				# Close Database
				DB_Module.close()

				# Log
				RecordedMessage = "New module detected, recording... [" + str(New_Module.Module_ID) + "]"
				LOG.Service_Logger.debug(RecordedMessage)

			else:

				# LOG
				LOG.Service_Logger.warning("Module allready recorded, bypassing...")





























			# Commit Message
			Kafka_Consumer.commit()

			# End LOG
			LOG.Line()
			print("")
			print("")


	finally:
		
		LOG.Service_Logger.fatal("Error Accured !!")


# Handle All Message in Topic
Device_Parser()





