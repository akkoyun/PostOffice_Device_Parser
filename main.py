# Import Libraries
from Setup import LOG, Database, Schema, Models
from Setup.Config import APP_Settings
from kafka import KafkaConsumer
import numpy as np
import json

# Create DB Models
Database.Base.metadata.create_all(bind=Database.DB_Engine)

# Kafka Consumer
Kafka_Consumer = KafkaConsumer('Device', bootstrap_servers=f"{APP_Settings.POSTOFFICE_KAFKA_HOSTNAME}:{APP_Settings.POSTOFFICE_KAFKA_PORT}", group_id="Data_Consumer", auto_offset_reset='earliest', enable_auto_commit=False)

# Boot Log Message
LOG.Service_Start()

def index(a_list, value):
    try:
        return a_list.index(value)
    except ValueError:
        return None

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
			class Variables:
				Module_ID = 0		# Module ID 
				IoT_Module_ID = 0	# GSM Module ID

			# ------------------------------------------

			# Define DB
			DB_Module = Database.SessionLocal()

			# Database Query
			Query_Module = DB_Module.query(Models.Module).filter(Models.Module.Device_ID.like(Device_ID)).first()

			# Handle Record
			if not Query_Module:

				# Create Add Record Command
				New_Module = Models.Module(Device_ID = Device_ID)

				# Add and Refresh DataBase
				DB_Module.add(New_Module)
				DB_Module.commit()
				DB_Module.refresh(New_Module)

				# Set Variable
				Variables.Module_ID = New_Module.Module_ID

				# Log
				LOG.Service_Logger.debug(f"New module detected, recording... [{New_Module.Module_ID}]")

			else:

				# Set Variable
				Module_Array = np.array(list(Query_Module.__dict__.items()))
				print(Module_Array)

				# LOG
				LOG.Service_Logger.warning(f"Module allready recorded [{Variables.Module_ID}], bypassing...")

			# Close Database
			DB_Module.close()

			# ------------------------------------------

			# Parse Version
			if Kafka_Message.Info.Firmware != None and Kafka_Message.Info.Hardware != None:

				# Define DB
				DB_Version = Database.SessionLocal()

				# Database Query
				Query_Version = DB_Version.query(Models.Version).filter(
					Models.Version.Device_ID.like(Device_ID),
					Models.Version.Firmware_Version.like(Kafka_Message.Info.Firmware),
					Models.Version.Hardware_Version.like(Kafka_Message.Info.Hardware)).first()

				# Handle Record
				if not Query_Version:

					# Create Add Record Command
					New_Version = Models.Version(
						Device_ID = Device_ID, 
						Hardware_Version = Kafka_Message.Info.Hardware,
						Firmware_Version = Kafka_Message.Info.Firmware)

					# Add and Refresh DataBase
					DB_Version.add(New_Version)
					DB_Version.commit()
					DB_Version.refresh(New_Version)

					# Log 
					LOG.Service_Logger.debug(f"New version detected, recording... [{New_Version.Version_ID}]")

				else:

					# LOG
					LOG.Service_Logger.warning("Version allready recorded, bypassing...")

				# Close Database
				DB_Version.close()

			else:

				# LOG
				LOG.Service_Logger.warning("There is no version info, bypassing...")

			# Close Database
			DB_Version.close()

			# ------------------------------------------

			# Parse IMU Data
			if Kafka_Message.Info.Temperature is not None and Kafka_Message.Info.Humidity is not None:

				# Define DB
				DB_IMU = Database.SessionLocal()

				# Create Add Record Command
				New_IMU = Models.IMU(
					Device_ID = Device_ID,
					Temperature = Kafka_Message.Info.Temperature,
					Humidity = Kafka_Message.Info.Humidity)

				# Add and Refresh DataBase
				DB_IMU.add(New_IMU)
				DB_IMU.commit()
				DB_IMU.refresh(New_IMU)

				# Log 
				LOG.Service_Logger.debug(f"New IMU data detected [{round(Kafka_Message.Info.Temperature, 2)} C / {round(Kafka_Message.Info.Humidity, 2)} %], recording... [{New_IMU.IMU_ID}]")

			else:

				# LOG
				LOG.Service_Logger.warning("There is no IMU data, bypassing...")

			# Close Database
			DB_IMU.close()

			# ------------------------------------------

			# Parse IoT Module
			if Kafka_Message.IoT.GSM.Module is not None:

				# Define DB
				DB_IoT_Module = Database.SessionLocal()

				# Database Query
				Query_IoT_Module = DB_IoT_Module.query(Models.IoT_Module).filter(
					Models.IoT_Module.Module_Firmware.like(Kafka_Message.IoT.GSM.Module.Firmware),
					Models.IoT_Module.Module_IMEI.like(Kafka_Message.IoT.GSM.Module.IMEI),
					Models.IoT_Module.Module_Serial.like(Kafka_Message.IoT.GSM.Module.Serial),
					Models.IoT_Module.Manufacturer_ID == Kafka_Message.IoT.GSM.Module.Manufacturer,
					Models.IoT_Module.Model_ID == Kafka_Message.IoT.GSM.Module.Model).first()

				# Handle Record
				if not Query_IoT_Module:

					# Create Add Record Command
					New_IoT_Module = Models.IoT_Module(
						Module_Type = 1,
						Module_Firmware = Kafka_Message.IoT.GSM.Module.Firmware,
						Module_IMEI = Kafka_Message.IoT.GSM.Module.IMEI,
						Module_Serial = Kafka_Message.IoT.GSM.Module.Serial,
						Manufacturer_ID = Kafka_Message.IoT.GSM.Module.Manufacturer,
						Model_ID = Kafka_Message.IoT.GSM.Module.Model)

					# Add and Refresh DataBase
					DB_IoT_Module.add(New_IoT_Module)
					DB_IoT_Module.commit()
					DB_IoT_Module.refresh(New_IoT_Module)

					# Set Variable
					Variables.IoT_Module_ID = New_IoT_Module.Module_ID

					# Log
					LOG.Service_Logger.debug(f"New IoT module detected, recording... [{New_IoT_Module.Module_ID}]")

				else:

					# Set Variable
					Variables.IoT_Module_ID = np.array(list(Query_IoT_Module.__dict__.items()))[6,1]

					# LOG
					LOG.Service_Logger.warning(f"IoT module allready recorded [{Variables.IoT_Module_ID}], bypassing...")

			else:

				# LOG
				LOG.Service_Logger.warning("There is no IoT module data, bypassing...")

			# Close Database
			DB_IoT_Module.close()

			# ------------------------------------------

			# Parse IoT Location
			if Kafka_Message.IoT.GSM.Operator.LAC is not None and Kafka_Message.IoT.GSM.Operator.Cell_ID is not None:

				# Define DB
				DB_Location = Database.SessionLocal()

				# Create Add Record Command
				New_IoT_Location_Post = Models.Location(
					Device_ID = Device_ID,
					LAC = Kafka_Message.IoT.GSM.Operator.LAC,
					Cell_ID = Kafka_Message.IoT.GSM.Operator.Cell_ID)

				# Add and Refresh DataBase
				DB_Location.add(New_IoT_Location_Post)
				DB_Location.commit()
				DB_Location.refresh(New_IoT_Location_Post)

				# LOG
				LOG.Service_Logger.debug(f"New location detected [{Kafka_Message.IoT.GSM.Operator.LAC} - {Kafka_Message.IoT.GSM.Operator.Cell_ID}], recording... [{New_IoT_Location_Post.Location_ID}]")

			else:

				# LOG
				LOG.Service_Logger.warning("There is no location data, bypassing...")

			# Close Database
			DB_Location.close()

			# ------------------------------------------



























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





