# Import Libraries
from Setup import Database, Schema, Models
from Setup.Config import APP_Settings
from datetime import datetime
import numpy as np

# Module Parser Function
async def Module_Parser(Headers, Variables, Service_Logger, ):

    # Define DB
    DB_Module = Database.SessionLocal()

    # Database Query
    Query_Module = DB_Module.query(Models.Module).filter(Models.Module.Device_ID.like(Headers.Device_ID)).first()

    # Handle Record
    if not Query_Module:

        # Create Add Record Command
        New_Module = Models.Module(
            Device_ID = Headers.Device_ID,
            Last_Online_Time = datetime.now(),
            Data_Count = 1)

        # Add and Refresh DataBase
        DB_Module.add(New_Module)
        DB_Module.commit()
        DB_Module.refresh(New_Module)

        # Set Variable
        Variables.Module_ID = New_Module.Module_ID

        # Log
        Service_Logger.debug(f"New module detected, recording... [{New_Module.Module_ID}]")

    else:

        # Set Variable
        for X in np.array(list(Query_Module.__dict__.items())):
            if X[0] == "Module_ID":
                Variables.Module_ID = X[1]
                break

        # Update Online Time
        setattr(Query_Module, 'Last_Online_Time', datetime.now())
        setattr(Query_Module, 'Data_Count', (Query_Module.Data_Count + 1))
        DB_Module.commit()

        # LOG
        Service_Logger.warning(f"Module allready recorded [{Variables.Module_ID}], bypassing...")

    # Close Database
    DB_Module.close()