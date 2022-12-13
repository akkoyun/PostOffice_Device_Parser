# Import Libraries
from Setup import LOG, Database, Schema, Models
from Setup.Config import APP_Settings
from kafka import KafkaConsumer
import json

# Create DB Models
Database.Base.metadata.create_all(bind=Database.DB_Engine)

# Kafka Consumer
Kafka_Consumer = KafkaConsumer('Device', bootstrap_servers=f"{APP_Settings.POSTOFFICE_KAFKA_HOSTNAME}:{APP_Settings.POSTOFFICE_KAFKA_PORT}", group_id="Data_Consumer", auto_offset_reset='earliest', enable_auto_commit=False)

# Boot Log Message
LOG.Service_Start()

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
			LOG.Line()
			LOG.Kafka_Header(Command, Device_ID, Device_IP, Device_Time, Message.topic, Message.partition, Message.offset)
			LOG.Line()

			# Handle Version
			if Kafka_Message.Info.Firmware != None and Kafka_Message.Info.Hardware != None:

				# Define DB
				db = Database.SessionLocal()

				# Database Query
				Version_Query = db.query(Models.Version).filter(
					Models.Version.Device_ID.like(Device_ID),
					Models.Version.Firmware_Version.like(Kafka_Message.Info.Firmware),
					Models.Version.Hardware_Version.like(Kafka_Message.Info.Hardware)).first()

				# Handle Record
				if Version_Query == None:

					# Create Add Record Command
					New_Version_Post = Models.Version(
						Device_ID = Device_ID, 
						Hardware_Version = Kafka_Message.Info.Hardware,
						Firmware_Version = Kafka_Message.Info.Firmware)

					# Add and Refresh DataBase
					db = Database.SessionLocal()
					db.add(New_Version_Post)
					db.commit()
					db.refresh(New_Version_Post)

					# Log 
					RecordedMessage = "Detected new version, recording... [" + str(New_Version_Post.Version_ID) + "]"
					LOG.Service_Logger.debug(RecordedMessage)

				else:

					# Log 
					LOG.Service_Logger.warning("Version allready recorded, bypassing...")
			
			else:

				# Log 
				LOG.Service_Logger.warning("There is no version info, bypassing...")









			# Print LOG
#			print(Kafka_Message)
			LOG.Service_Logger.info(Kafka_Message)
#			print("Hardware Version : ", Kafka_Message.Hardware)
#			print("Firmware Version : ", Kafka_Message.Firmware)
#			print("................................................................................")

			# Declare db
#			db = Database.SessionLocal()



			# Close Database
#			db.close()

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





