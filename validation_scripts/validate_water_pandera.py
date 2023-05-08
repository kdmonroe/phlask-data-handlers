import os 
import sys
from datetime import date
from pathlib import Path
import pandas as pd
import numpy as np
import pandera as pa

# import panderas schema from schema_water.py
from schema_water import schema

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
backend_dir = os.path.join(backend_dir, "dashboard", "backend")


# Add the project root directory to the Python path
sys.path.insert(-1,backend_dir)

from admin_classes import prodAdmin, betaAdmin, testAdmin

def main():
    ''' WATER DATA VALIDATION SCRIPT
        This script validates the water data in the prod, beta, and test environments.
        It uses the pandera library to validate the data against a schema.
        
    '''
    # Initialize the prod, beta, and test environment database references.
    water_prod = prodAdmin().water_db_live
    water_beta = betaAdmin().water_db_live
    water_test = testAdmin().water_db_live

    project_root = Path(__file__).resolve().parent.parent

    # Validate resource data in each environment
    for i, (db, db_name) in enumerate([(water_prod, 'water_prod'), (water_beta, 'water_beta'), (water_test, 'water_test')]):
        if i == 0:
            print(f"\n\t{i + 1}. Validating data in {db_name} database...")
            
            if i == 0:
                admin_class = prodAdmin
            elif i == 1:
                admin_class = betaAdmin
            else:
                admin_class = testAdmin

            # Retrieve data from database and load as pandas dataframe
            data = admin_class().getDb(db)
            df = pd.DataFrame(data)
            print(f"\t\t- {len(df)} rows retrieved from {db_name} database.")

            # drop any data w/o tapnum
            df = df.iloc[df.dropna(subset=['tapnum']).index]
            print(f"\t\t- {len(df)} rows after dropping rows without tapnum.")

            # Initialize err variable for panderas schema validation 
            err = None
        
            try:
                schema.validate(df, lazy=True)
                print("\t\t- No errors found.")
            except pa.errors.SchemaErrors as e:
                err = e
                print("\n\tSchema errors and failure cases:")
                for error in err.failure_cases:
                    print(f"\t\t- {error}")

                # Write validation errors data to a .csv file
                df_errors = pd.DataFrame(err.data)
                out_csv = project_root / "validation_scripts" / "validation_results" / f"{db_name}_errors.csv"
                
                print("\n\n- Exporting Pandera Validation Errors to .csv file...")
                if out_csv.exists():
                    os.remove(out_csv)
                    print(f"\t\t- Removed Existing {out_csv}")

                df_errors.to_csv(out_csv, index=False)
                print(f"\t\t- Exported Pandera Validation Errors to {out_csv}...")

            # Manual basic insights about the data w/ pandas 
            null_percentages = df.isnull().mean().round(4) * 100
            
            most_common_city = df['city'].mode().iloc[0]
            print(f"\t\t- Most common city: {most_common_city}")

            status_counts = df['status'].value_counts()
            print(f"\t\t- Total number of rows for each unique value in 'status':\n{status_counts}")

            access_counts = df['access'].value_counts()
            print(f"\t\t- Total number of rows for each unique value in 'access':\n{access_counts}")

            filtration_counts = df['filtration'].value_counts()
            print(f"\t\t- Total number of rows for each unique value in 'filtration':\n{filtration_counts}")

            # get most common zip code - dont count empty strings
            zip_code_counts = df['zip_code'].replace('', np.nan).dropna().value_counts()
            most_common_zip_code = zip_code_counts.idxmax()
            print(f"\t\t- Most common zip code: {most_common_zip_code}")

            image_count = df['images'].notnull().sum()
            print(f"\t\t- Number w/ tap images: {image_count}")

            # Write validation results to a .txt file
            today = date.today().strftime("%Y-%m-%d")
            output_dir = project_root / "validation_scripts" / "validation_results"
            results_txt = output_dir / f"{db_name}_{today}.txt"

            output_dir.mkdir(parents=True, exist_ok=True)
            print(f"\t\t- Writing validation results to {results_txt}...")

            # get unique index values for rows that failed validation
            unique_index = df_errors.index.unique().tolist()
            print(f"\t\t- {len(unique_index)} rows failed validation.")
            if unique_index:
                print(f"Failures at Table Index(es): {unique_index}")

            with open(results_txt, "w") as f:
                f.write(f"Validation results for {db_name} database on {today}:\n")
                f.write(f"\t- Number of rows validated: {len(df)}\n")
                if err:
                    f.write(f"\t- Number of rows with errors: {len(err.failure_cases)}\n")
                    f.write(f"Rows at index {unique_index} failed validation.\n")

                    f.write("\t- Error messages:\n")
                    for error in err.failure_cases:
                        f.write(f"\t\t- {error}\n")
                else:
                    f.write("\t- Number of rows with errors: 0\n")
                    f.write("\t- No errors found.\n")
                
                f.write("\nBasic insights about the data:\n")
                f.write("\t\t* Data was filtered to remove rows without tapnum.\n")
                f.write(f"\n- Most common city: {most_common_city}\n")
                f.write(f"\n- Total number of rows for each unique value in 'access':\n{access_counts}\n")
                f.write(f"\n- Total number of rows for each unique value in 'filtration':\n{filtration_counts}\n")
                f.write(f"\n- Total number of rows for each unique value in 'status':\n{status_counts}\n")
                f.write(f"\n- Most common zip code: {most_common_zip_code}\n")
                f.write(f"\n- Number of rows with images: {image_count}\n")
                f.write("\n- Percentage of null values for each field:\n")
                for field, percentage in null_percentages.items():
                    f.write(f"\t\t- {field}: {percentage}%\n")

if __name__ == "__main__":
    main()
