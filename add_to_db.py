import csv
import glob
import sqlite3

print("This isn't working yet!")
exit(0)

con = sqlite3.connect('census.db')

cur = con.cursor()

def csv_iter(filename):
    with open(filename, newline='') as f:
        csvreader = csv.reader(f)
        for i, row in enumerate(csvreader):
            if i == 0:
                headings = row
            else:
                yield dict(zip(headings, row))

tables = [
    "COUNTS(place_id, year, val_id, count)",
    "VARIABLES(var_name, population, nomis_table_code_2011)",
    "CATEGORIES(var_id, val_name, measurement_unit, stat_unit, nomis_code_2011)"
]

for table in tables:
    cur.execute('DROP TABLE IF EXISTS {}'.format(table[:table.find("(")]))
for table in tables:
    cur.execute('CREATE TABLE IF NOT EXISTS {}'.format(table))

nomis_table_id_to_var_id = {}

meta_files = glob.glob("data/**/*META*.CSV", recursive=True)
for filename in meta_files:
    for d in csv_iter(filename):
        print(d)
        cur.execute('insert into VARIABLES values (?,?,?)',
            [d["DatasetTitle"], d["StatisticalPopulations"], d["DatasetId"]])
        nomis_table_id_to_var_id[d["DatasetId"]] = cur.lastrowid
    print()

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

print(nomis_table_id_to_var_id)

data01_files = glob.glob("data/**/*DATA01.CSV", recursive=True)
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
#"COUNTS(place_id, year, val_id, count)"
#"CATEGORIES(var_id, val_name, measurement_unit, stat_unit, nomis_code_2011)"

con.commit()

con.close()

