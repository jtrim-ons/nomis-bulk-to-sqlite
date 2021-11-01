import csv
import glob
import sqlite3

TABLES = [
    "COUNTS(place_id, year, category_id, count)",
    "VARIABLES(var_name, population, nomis_table_code_2011)",
    "CATEGORIES(var_id, val_name, measurement_unit, stat_unit, nomis_code_2011)"
]

def csv_iter(filename):
    with open(filename, newline='') as f:
        yield from csv.DictReader(f)

def create_tables(cur):
    for table in TABLES:
        cur.execute('DROP TABLE IF EXISTS {}'.format(table[:table.find("(")]))
    for table in TABLES:
        cur.execute('CREATE TABLE IF NOT EXISTS {}'.format(table))

def add_meta_tables(cur):
    nomis_table_id_to_var_id = {}
    meta_files = glob.glob("data/**/*META*.CSV", recursive=True)
    for filename in meta_files:
        for d in csv_iter(filename):
            print(d)
            cur.execute('insert into VARIABLES values (?,?,?)',
                [d["DatasetTitle"], d["StatisticalPopulations"], d["DatasetId"]])
            nomis_table_id_to_var_id[d["DatasetId"]] = cur.lastrowid
        print()
    return nomis_table_id_to_var_id

def add_desc_tables(cur, nomis_table_id_to_var_id):
    nomis_col_id_to_category_id = {}
    desc_files = glob.glob("data/**/*DESC*.CSV", recursive=True)
    print(desc_files)
    for filename in desc_files:
        for d in csv_iter(filename):
            print(d)
            col_code = d["ColumnVariableCode"]
            pos = col_code.rfind("EW")
            if pos == -1:
                continue
            table_code = col_code[:pos+2]
            print("TABLECODE", table_code)
            var_id = nomis_table_id_to_var_id[table_code]
            cur.execute('insert into CATEGORIES values (?,?,?,?,?)', [
                var_id,
                d["ColumnVariableDescription"],
                d["ColumnVariableMeasurementUnit"],
                d["ColumnVariableStatisticalUnit"],
                d["ColumnVariableCode"]
            ])
            nomis_col_id_to_category_id[d["ColumnVariableCode"]] = cur.lastrowid
        print()
    return nomis_col_id_to_category_id

def add_data_tables(cur, nomis_col_id_to_category_id):
    for data_file_num in ["01", "02", "03", "04", "05", "06"]:
        data_files = glob.glob("data/**/*DATA{}.CSV".format(data_file_num), recursive=True)
        print(data_files)
        for filename in data_files:
            print(filename)
            rows = []
            for d in csv_iter(filename):
                geog_code = d["GeographyCode"]
                for column_code in d:
                    if column_code == "GeographyCode":
                        continue
                    if column_code not in nomis_col_id_to_category_id:
                        continue
                    rows.append((
                        geog_code,
                        2011,
                        nomis_col_id_to_category_id[column_code],
                        d[column_code]
                    ))
            cur.executemany('insert into COUNTS values (?,?,?,?)', rows)

def main():
    con = sqlite3.connect('census.db')
    cur = con.cursor()

    create_tables(cur)

    nomis_table_id_to_var_id = add_meta_tables(cur)
    nomis_col_id_to_category_id = add_desc_tables(cur, nomis_table_id_to_var_id)
    add_data_tables(cur, nomis_col_id_to_category_id)

    con.commit()
    con.close()

if __name__ == "__main__":
    main()
