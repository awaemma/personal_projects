# THE SELF SERVICE DATA BACKFILL 
Following the increasing adoption of the self-service dashboard within the Technology team and beyond, it has become imperative that certain standardization be introduced to ensure high data availability and accuracy. One of such is to partially automate the backfill of data in cases where there are no successfully insertion to the fact_tbl table in the self-serviceDB due to several reasons which includes but not limited to server unavailability or db connection error at runtime.

## How to run the code
Pip install `requirement.txt` if you do not the dependencies. This should work fine with default python packages.
In the current implementation, you are required to pass in 2 parameters to the `main.py` file.
   - The script file name found in the location `D:\SelfService_ETL\backfill\script`
   - The connection type depending on what data you are backfilling found at `db_manager.py`.

First locate the `.sql` script of choice in the location `D:\SelfService_ETL\backfill\script` and edit in a notepad to change the date to your preferred date. (In a revised version in future, the start and end dates will also be passed as paramteres. For now, this is what we got.)
Then run `python main.py <first param> <second param>`

## Example usage
If you want to backfill data for inward conventional, Do the following
   - Locate the inward conventional script in the location `D:\SelfService_ETL\backfill\script` to pick exact file name.
   - Run `python main.py "inward_conventional_backfill" "inward"`

If you get an error similar to `Database configuration is not found`, it means the database connection has not been added to the `db_manager.py` file.





