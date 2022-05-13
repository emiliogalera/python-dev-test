import sys
from tools import data_interaction
from tools import db_handler

if __name__ == "__main__":
    batch = int(sys.argv[1]) # to tired to use argparse
    # creates the data_handler to interact with the raw data
    data_handler = data_interaction.DataHandler("data/Adult.data")

    # make a batch of data to process in this run
    flag, treated_data = data_handler.make(batch)

    #get columns to create the table
    columns = treated_data.columns

    # db name
    database_name = f"ready_to_process_{batch}.sqlite"

    #table name
    table_name = "production_data"

    # every thing has been transformed to numbers to allow different types of processing.
    database = db_handler.DbHandler(database_name)

    # cleaning some characteres in column names that sqlite does not like
    database_fields = {}
    for k in columns:
        if "-" in k:
            k = k.replace("-", "_")
        if "(" in k or ")" in k:
            k = k.replace("(", "_")
            k = k.replace(")", "_")
        if ">" in k:
            k = k.replace(">", "plus_")
        if "<=" in k:
            k = k.replace("<=", "less_eq_")
        if "&" in k:
            k = k.replace("&", "_and_")
        database_fields[k] = float

    database.create_table(table_name, database_fields)

    # insert cleaned data on database
    print(f"Processing batch: {batch}")
    for idx, data in treated_data.iterrows():
        database.insert(table_name, data.values, commit=False)
        print(f"    inserting to production_{batch}: {idx + 1} of {treated_data.shape[0]}")

    database.commit()

    # flag the last batch to stop the bash script
    if not flag:
        with open("StopSign", "w") as f:
            f.write("THE END")
