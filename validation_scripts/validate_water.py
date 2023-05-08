import sys
import os
import json
# import logging
import csv
from datetime import date
import pandas as pd
import pandera as pa 

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Add the project root directory to the Python path
sys.path.insert(-1,project_root)

from admin_classes import prodAdmin, betaAdmin, testAdmin

# Set up logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define water schema as a class object
class WaterSchema:
    access: str
    address: str
    city: str
    description: str
    filtration: str
    gp_id: str
    handicap: str
    hours: list
    lat: float
    lon: float
    norms_rules: str
    organization: str
    permanently_closed: bool
    phone: str
    quality: str
    service: str
    statement: str
    status: str
    tap_type: str
    tapnum: int
    vessel: str
    zip_code: str

def validate_json(data, error_count, warning_count, unexpected_count):
    ''' Validates a JSON object against a schema
        Returns the number of errors, warnings, and unexpected types found
    '''
    expected_data_types = WaterSchema.__annotations__

    for key, value in data.items():
        if key in expected_data_types:
            if not isinstance(value, expected_data_types[key]):
                logger.error(f"Invalid data type for key '{key}': Expected {expected_data_types[key]}, got {type(value)}")
                error_count += 1
        elif key not in expected_data_types:
            logger.error(f"Unexpected key '{key}'")
            error_count += 1
        else:
            logger.warning(f"Unexpected key '{key}'")
            warning_count += 1

        if isinstance(value, dict):
            error_count, warning_count, unexpected_count = validate_json(value, error_count, warning_count, unexpected_count)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    error_count, warning_count, unexpected_count = validate_json(item, error_count, warning_count, unexpected_count)
                else:
                    unexpected_count += 1

    return error_count, warning_count, unexpected_count

def validate_data(admin_class, db, db_name):
    ''' Retrieves data from a database and validates it against a schema 
        Saves the results to a csv file
    '''
    data = admin_class().getDb(db)
    print(f"\nData retrieved from {db}")
    print(f"\n\tLength of dataset: {len(data)}")

    # Validate data
    error_count = 0
    warning_count = 0
    unexpected_count = 0
    for x in data:
        e, w, u = validate_json(x, error_count, warning_count, unexpected_count)
        error_count += e
        warning_count += w
        unexpected_count += u

    logging.info(f"Errors: {error_count}")
    logging.info(f"Warnings: {warning_count}")
    logging.info(f"Unexpected Types: {unexpected_count}")

    # Create dataframe with results of validation
    df_results = pd.DataFrame({
        "Database": [db_name],
        "Errors": [error_count],
        "Warnings": [warning_count],
        "Unexpected Types": [unexpected_count]}) 

    logging.info(f"Length of dataset: {len(data)}")

    # Write report of all the logging with total counts
    today = date.today()
    csv_file_path = f"{today.strftime('%Y-%m-%d')}_firebase_db_validation_{db_name}.csv"

    # check if csv file exists and if so delete it
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)

    # save as csv and text file dated with today's date
    df_results.to_csv(csv_file_path, index=False)
    logging.info(f"Validation results saved to {csv_file_path}")


def main():
    # Initialize the prod, beta, and test environment database references.
    logging.info("Initializing database references...")
    water_prod = prodAdmin().water_db_live
    water_beta = betaAdmin().water_db_live
    water_test = testAdmin().water_db_live

    # Validate data in each database - WATER TAPS DATA
    for i, (db, db_name) in enumerate([(water_prod, 'water_prod'), (water_beta, 'water_beta'), (water_test, 'water_test')]):
        logging.info(f"\n\t{i + 1}. Validating data in {db_name} database...")
        
        if i == 0:
            admin_class = prodAdmin
        elif i == 1:
            admin_class = betaAdmin
        else:
            admin_class = testAdmin

        # Retrieve data from database
        data = admin_class().getDb(db)
        logging.info(f"\nData retrieved from {db}")
        logging.info(f"\n\tLength of dataset: {len(data)}")

        # validate against schema - get total errors, warnings, and unexpected types
        validate_data(admin_class, db, db_name)
        break




if __name__ == "__main__":
    main()

