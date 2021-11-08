import csv
import json
import sqlite3

def process_items(indicators, cur):
    for item in indicators:
        if "code" in item and len(item["code"]) == 10 and item["code"][:2] in ["KS", "QS"]:
            code = item["code"]
            table = code[:5]
            column = int(code[-3:]) + 1
            code = "{}EW{:04}".format(table, column)
            cur.execute('select * from CATEGORIES where nomis_code_2011 = ?', (code,))
            db_row = cur.fetchone()
            if db_row is None:
                continue
            if db_row[1] != item["name"] and db_row[1] not in item["name"] and item["name"] not in db_row[1]:
                print("---", code, "--", db_row[1], "--", item["name"])

        if "children" in item:
            process_items(item["children"], cur)

def main():
    con = sqlite3.connect('census.db')
    cur = con.cursor()

    with open("indicators/indicators.json", "r") as f:
        indicators = json.load(f)

    process_items(indicators, cur)

    con.commit()
    con.close()

if __name__ == "__main__":
    main()
