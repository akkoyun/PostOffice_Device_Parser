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

			# Parse Device
			if Kafka_Message.Info != None:

				# Define DB
				db = Database.SessionLocal()

				# Database Query
				IoT_Module_Query = db.query(Models.Module).filter(Models.Module.Device_ID.like(Device_ID)).first()

				# Handle Record
				if IoT_Module_Query == None:

					# Create Add Record Command
					New_Module = Models.Module(
						Device_ID = Device_ID,
						Device_Development = True,
						Module_Name = "B100xx")

					# Add and Refresh DataBase
					db.add(New_Module)
					db.commit()
					db.refresh(New_Module)

					# Log
					RecordedMessage = "New module detected, recording... [" + str(New_Module.Module_ID) + "]"
					LOG.Service_Logger.debug(RecordedMessage)

				else:
					LOG.Service_Logger.warning("Module allready recorded, bypassing...")
			else:
				LOG.Service_Logger.warning("There is no info, bypassing...")

			# Parse Version
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
					New_Version = Models.Version(
						Device_ID = Device_ID, 
						Hardware_Version = Kafka_Message.Info.Hardware,
						Firmware_Version = Kafka_Message.Info.Firmware)

					# Add and Refresh DataBase
					db.add(New_Version)
					db.commit()
					db.refresh(New_Version)

					# Log 
					RecordedMessage = "Detected new version, recording... [" + str(New_Version.Version_ID) + "]"
					LOG.Service_Logger.debug(RecordedMessage)

				else:
					LOG.Service_Logger.warning("Version allready recorded, bypassing...")
			else:
				LOG.Service_Logger.warning("There is no version info, bypassing...")

			# Parse IMU Data
			if Kafka_Message.Info.Temperature is not None and Kafka_Message.Info.Humidity is not None:

				# Define DB
				db = Database.SessionLocal()

				# Create Add Record Command
				New_IMU = Models.IMU(
					Device_ID = Device_ID,
					Temperature = Kafka_Message.Info.Temperature,
					Humidity = Kafka_Message.Info.Humidity)

				# Add and Refresh DataBase
				db.add(New_IMU)
				db.commit()
				db.refresh(New_IMU)

				# Log
				RecordedMessage = "Detected new IMU data, recording... [" + str(New_IMU.IMU_ID) + "]"
				LOG.Service_Logger.debug(RecordedMessage)
			else:
				LOG.Service_Logger.warning("There is no IMU data, bypassing...")

			# Parse IoT Module
			if Kafka_Message.IoT.GSM.Module is not None:

				# Define DB
				db = Database.SessionLocal()

				# Database Query
				IoT_Module_Query = db.query(Models.IoT_Module).filter(
					Models.IoT_Module.Module_Firmware.like(Kafka_Message.IoT.GSM.Module.Firmware),
					Models.IoT_Module.Module_IMEI.like(Kafka_Message.IoT.GSM.Module.IMEI),
					Models.IoT_Module.Module_Serial.like(Kafka_Message.IoT.GSM.Module.Serial),
					Models.IoT_Module.Manufacturer_ID == Kafka_Message.IoT.GSM.Module.Manufacturer,
					Models.IoT_Module.Model_ID == Kafka_Message.IoT.GSM.Module.Model).first()

				# Handle Record
				if IoT_Module_Query == None:

					# Create Add Record Command
					New_IoT_Module = Models.IoT_Module(
						Module_Type = 1,
						Module_Firmware = Kafka_Message.IoT.GSM.Module.Firmware,
						Module_IMEI = Kafka_Message.IoT.GSM.Module.IMEI,
						Module_Serial = Kafka_Message.IoT.GSM.Module.Serial,
						Manufacturer_ID = Kafka_Message.IoT.GSM.Module.Manufacturer,
						Model_ID = Kafka_Message.IoT.GSM.Module.Model)

					# Add and Refresh DataBase
					db.add(New_IoT_Module)
					db.commit()
					db.refresh(New_IoT_Module)

					# Log
					RecordedMessage = "Detected new IoT module, recording... [" + str(New_IoT_Module.Module_ID) + "]"
					LOG.Service_Logger.debug(RecordedMessage)
				else:
					LOG.Service_Logger.warning("IoT module allready recorded, bypassing...")
			else:
				LOG.Service_Logger.warning("There is no IoT module data, bypassing...")

			# Parse IoT Location
			if Kafka_Message.IoT.GSM.Operator.LAC is not None and Kafka_Message.IoT.GSM.Operator.Cell_ID is not None:

				# Define DB
				db = Database.SessionLocal()

				# Create Add Record Command
				New_IoT_Location_Post = Models.Location(
					Device_ID = Device_ID,
					LAC = Kafka_Message.IoT.GSM.Operator.LAC,
					Cell_ID = Kafka_Message.IoT.GSM.Operator.Cell_ID)

				# Add and Refresh DataBase
				db.add(New_IoT_Location_Post)
				db.commit()
				db.refresh(New_IoT_Location_Post)

				# Log
				RecordedMessage = "Detected new location, recording... [" + str(New_IoT_Location_Post.Location_ID) + "]"
				LOG.Service_Logger.debug(RecordedMessage)
			else:
				LOG.Service_Logger.warning("There is no location data, bypassing...")

			# Parse IoT SIM
			if Kafka_Message.IoT.GSM.Operator.ICCID is not None:

				# Define DB
				db = Database.SessionLocal()

				# Database Query
				IoT_SIM_Query = db.query(Models.SIM).filter(
					Models.SIM.ICCID.like(Kafka_Message.IoT.GSM.Operator.ICCID),
					Models.SIM.Operator_ID == Kafka_Message.IoT.GSM.Operator.Code).first()

				# Handle Record
				if IoT_SIM_Query is None:

					# Create Add Record Command
					New_IoT_SIM_Post = Models.SIM(
						Device_ID = Device_ID,
						ICCID = Kafka_Message.IoT.GSM.Operator.ICCID,
						Operator_ID = Kafka_Message.IoT.GSM.Operator.Code)

					# Add and Refresh DataBase
					db.add(New_IoT_SIM_Post)
					db.commit()
					db.refresh(New_IoT_SIM_Post)

					# Log
					RecordedMessage = "Detected new SIM, recording... [" + str(New_IoT_SIM_Post.SIM_ID) + "]"
					LOG.Service_Logger.debug(RecordedMessage)
				else:
					LOG.Service_Logger.warning("SIM allready recorded, bypassing...")
			else:
				LOG.Service_Logger.warning("There is no SIM data, bypassing...")

			# Parse IoT Connection
			if Kafka_Message.IoT.GSM.Operator.RSSI is not None:

				# Define DB
				db = Database.SessionLocal()

				# Database Query
				IoT_Existing_SIM_Query = db.query(Models.SIM).filter(
					Models.SIM.ICCID.like(Kafka_Message.IoT.GSM.Operator.ICCID),
					Models.SIM.Device_ID.like(Device_ID)).first()

				# Refresh DataBase
				db.refresh(IoT_Existing_SIM_Query)

				# Create Add Record Command
				New_IoT_Connection_Post = Models.Connection(
					Device_ID = Device_ID,
					SIM_ID = IoT_Existing_SIM_Query.SIM_ID,
					RSSI = Kafka_Message.IoT.GSM.Operator.RSSI,
					Device_IP = Kafka_Message.IoT.GSM.Operator.IP,
					Connection_Time = Kafka_Message.IoT.GSM.Operator.ConnTime,
					Data_Size = len(Kafka_Message))

				# Add and Refresh DataBase
				db.add(New_IoT_Connection_Post)
				db.commit()
				db.refresh(New_IoT_Connection_Post)

				# Lof
				RecordedMessage = "Detected new connection data, recording... [" + str(New_IoT_Connection_Post.Connection_ID) + "]"
				LOG.Service_Logger.debug(RecordedMessage)
			else:
				LOG.Service_Logger.warning("There is no connection data, bypassing...")











			# Close Database
			db.close()

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





