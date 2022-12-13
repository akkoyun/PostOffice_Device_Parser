# Import Libraries
from Setup import Database, Schema, Models, log_functions
from Setup.Config import APP_Settings
from kafka import KafkaConsumer
import json

# Create DB Models
Database.Base.metadata.create_all(bind=Database.DB_Engine)

# Kafka Consumer
Kafka_Consumer = KafkaConsumer('Device', bootstrap_servers=f"{APP_Settings.POSTOFFICE_KAFKA_HOSTNAME}:{APP_Settings.POSTOFFICE_KAFKA_PORT}", group_id="Data_Consumer", auto_offset_reset='earliest', enable_auto_commit=False)

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
			log_functions.Log_Kafka_Header(Command, Device_ID, Device_IP, Device_Time, Message.topic, Message.partition, Message.offset)

			# Print LOG
			print(Kafka_Message)
#			print("Hardware Version : ", Kafka_Message.Hardware)
#			print("Firmware Version : ", Kafka_Message.Firmware)
#			print("................................................................................")

			# Declare db
#			db = Database.SessionLocal()

			# Handle Data
#			if Kafka_Message.Firmware != None and Kafka_Message.Hardware != None:

				# Database Query
#				Version_Query = db.query(Models.Version).filter(
#					Models.Version.Device_ID.like(Device_ID),
#					Models.Version.Firmware_Version.like(Kafka_Message.Firmware),
#					Models.Version.Hardware_Version.like(Kafka_Message.Hardware)).first()

				# Handle Record
#				if Version_Query == None:

					# Create Add Record Command
#					New_Version_Post = Models.Version(
#						Device_ID = Device_ID, 
#						Hardware_Version = Kafka_Message.Hardware,
#						Firmware_Version = Kafka_Message.Firmware)

#					# Add and Refresh DataBase
#					db = Database.SessionLocal()
#					db.add(New_Version_Post)
#					db.commit()
#					db.refresh(New_Version_Post)

					# Log 
#					print("There is no existing record for this device. Adding record : ", New_Version_Post.Version_ID)


#				else:
#					print("There is an existing record for this device. Bypassing...")

			# Close Database
#			db.close()

			# Commit Message
#			Kafka_Version_Consumer.commit()
#
			print("--------------------------------------------------------------------------------")


	finally:
		
		print("Error Accured !!")


# Handle All Message in Topic
Device_Parser()





