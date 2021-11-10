import csv
import glob
import sqlite3
from collections import namedtuple

TABLES = [
    '''COUNTS(
        count_id INTEGER PRIMARY KEY,
        place_id INTEGER,
        year INTEGER,
        category_id INTEGER,
        count INTEGER
    )''',
    '''VARIABLES(
        var_id INTEGER PRIMARY KEY,
        var_name TEXT,
        population TEXT,
        nomis_table_code_2011 TEXT
    )''',
    '''CATEGORIES(
        category_id INTEGER PRIMARY KEY,
        var_id INTEGER,
        category_name TEXT,
        measurement_unit TEXT,
        stat_unit TEXT,
        nomis_code_2011 TEXT
    )''',
    '''LSOA2011_LAD2020_LOOKUP(
        id INTEGER PRIMARY KEY,
        lsoa2011code TEXT,
        lad2020code TEXT
    )''',
    '''PLACE_TYPES(
        placetype_id INTEGER PRIMARY KEY,
        placetype_name TEXT
    )''',
    '''PLACES(
        place_id INTEGER PRIMARY KEY,
        place_code TEXT,
        place_name TEXT,
        placetype_id INTEGER
    )'''
]

PLACE_TYPES = {
    1: "EW",
    2: "Country",
    3: "Region",
    4: "LAD",
    5: "MSOA",
    6: "LSOA",
    99: "LAD2020_best_fit"
}

CategoryInfo = namedtuple('CategoryInfo', ['id', 'measurement_unit'])

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
            sql = '''insert into VARIABLES (var_name,population,nomis_table_code_2011)
                      values (?,?,?);
                    '''
            cur.execute(sql, [d["DatasetTitle"], d["StatisticalPopulations"], d["DatasetId"]])
            nomis_table_id_to_var_id[d["DatasetId"]] = cur.lastrowid
        print()
    return nomis_table_id_to_var_id

def add_desc_tables(cur, nomis_table_id_to_var_id):
    nomis_col_id_to_category_info = {}
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
            sql = '''insert into CATEGORIES (var_id,category_name,measurement_unit,stat_unit,nomis_code_2011)
                     values (?,?,?,?,?);
                  '''
            cur.execute(sql, [
                var_id,
                d["ColumnVariableDescription"],
                d["ColumnVariableMeasurementUnit"],
                d["ColumnVariableStatisticalUnit"],
                d["ColumnVariableCode"]
            ])
            nomis_col_id_to_category_info[d["ColumnVariableCode"]] = CategoryInfo(
                cur.lastrowid,
                d["ColumnVariableMeasurementUnit"]
            )
        print()
    return nomis_col_id_to_category_info

def add_counts(cur, rows, placetype_id, place_code_to_id):
    for row in rows:
        if row[0] not in place_code_to_id:
            # This place code isn't in the places table yet, so add it.
            sql = '''insert into PLACES (place_code,place_name,placetype_id)
                      values (?,?,?);
                    '''
            cur.execute(sql, (row[0], row[0] + " name TODO", placetype_id))
            place_code_to_id[row[0]] = cur.lastrowid
        row[0] = place_code_to_id[row[0]]   # replace code with ID
    sql = 'insert into COUNTS (place_id, year, category_id, count) values (?,?,?,?);'
    cur.executemany(sql, rows)   # Much faster than executemany

def add_data_tables(cur, nomis_col_id_to_category_info, place_code_to_id):
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
                    if column_code not in nomis_col_id_to_category_info:
                        continue
                    if nomis_col_id_to_category_info[column_code].measurement_unit != 'Count':
                        continue
                    rows.append([
                        geog_code,
                        2011,
                        nomis_col_id_to_category_info[column_code].id,
                        float(d[column_code])
                    ])
            add_counts(cur, rows, int(data_file_num), place_code_to_id)

def create_place_types(cur, place_types):
    for placetype_id, placetype_name in place_types.items():
        sql = 'insert into PLACE_TYPES (placetype_id,placetype_name) values (?,?)'
        cur.execute(sql, (placetype_id, placetype_name))

def add_lsoa_lad_lookup(cur):
    for d in csv_iter("lookup/lsoa2011_lad2020.csv"):
        lsoa = d["code"]
        lad = "best_fit_" + d["parent"]
        cur.execute('insert into LSOA2011_LAD2020_LOOKUP (lsoa2011code,lad2020code) values (?,?)', [lsoa, lad])

def add_best_fit_lad2020_rows(cur, place_code_to_id):
    cur.execute(
        '''select lad2020code, year, category_id, sum(count) from (
                select lad2020code, year, category_id, count from LSOA2011_LAD2020_LOOKUP
                    join PLACES on LSOA2011_LAD2020_LOOKUP.lsoa2011code = PLACES.place_code
                    join COUNTS on PLACES.place_id = COUNTS.place_id
            ) as A group by lad2020code, year, category_id;''')
    new_rows = [list(item) for item in cur.fetchall()]
    add_counts(cur, new_rows, 99, place_code_to_id)

def main():
    con = sqlite3.connect('census.db')
    cur = con.cursor()

    place_code_to_id = {}  # a map from place code (e.g. "E09000001") to place_id in the PLACES table

    create_tables(cur)
    create_place_types(cur, PLACE_TYPES)
    nomis_table_id_to_var_id = add_meta_tables(cur)
    nomis_col_id_to_category_info = add_desc_tables(cur, nomis_table_id_to_var_id)
    add_data_tables(cur, nomis_col_id_to_category_info, place_code_to_id)

    cur.execute('create index idx_counts_place_id on counts(place_id)')

    add_lsoa_lad_lookup(cur)

    add_best_fit_lad2020_rows(cur, place_code_to_id)

    cur.execute('create index idx_counts_category_id on counts(category_id)')

    con.commit()
    con.close()

if __name__ == "__main__":
    main()
