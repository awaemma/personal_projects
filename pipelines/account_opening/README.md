# ACCOUNT OPENING PIPELINE

This pipeline migrates data from the newbank source to the Datawarehouse for donward analytics.The folder holds the following files

- `__init__.py` which initializes the package
- `.env` this holds the credentials to the source and destination databases. This is included in the git ignore file
- `db_manager.py` holds the functions that helps to connect to both the source and destination dbs. As well as data download logic.
- `conventional_main.py` holds the logic to migrate data for conventional data
- `TAB_main.py` holds the logic to migrate data for TAB data
- `TAB_restriction_main.py` holds the logic to migrate data for TAB restricted account data 
- `orchestrator.py` manages how each of the other py files(conv and TAB files) are run.   

## HOW TO RUN THE CODE
 - Optionally, you may wish to create a python virtual environment.
 - Pip install the `requirements.txt` file if you already do not have all the packages installed
 - Create a .env file and set your connection variable. The `db_manager.py` file will give you an idea on what is required.
 - Run the `orchestrator.py` file. e.g `python orchestrator.py`
