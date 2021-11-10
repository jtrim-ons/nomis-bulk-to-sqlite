import csv
import sqlite3

from flask import Flask
from werkzeug.wrappers import Response

app = Flask(__name__)

@app.route('/')
def hello_world():
   return "Hello, World! v2"

def get_rows(code, cur):
    cur.execute('''select place_code, count
                    from counts join places on counts.place_id=places.place_id
                    where category_id = (select category_id from CATEGORIES where nomis_code_2011 = ?)''', (code,))
    rows = cur.fetchall()
    result = []
    for row in rows:
        if row[0].startswith("K04"):
            result.append(("EW", int(row[1])))
        elif row[0].startswith("E01") or row[0].startswith("W01"):
            result.append((row[0], int(row[1])))
        elif row[0].startswith("best_fit_"):
            result.append((row[0][9:], int(row[1])))
    return result

@app.route('/column/<code>', methods=['GET'])
def get_csv(code):
    con = sqlite3.connect('census.db')
    cur = con.cursor()

    table = code[:5]
    column = int(code[-3:]) + 1
    code1 = "{}EW{:04}".format(table, column)
    code2 = "{}EW{:04}".format(table, 1)
    rows = get_rows(code1, cur)
    total_rows = dict(get_rows(code2, cur))

    con.close()

    response = "GEOGRAPHY_CODE,TOTAL,COUNT\n"
    for row in rows:
        response += "{},{},{}\n".format(row[0], total_rows[row[0]], row[1])
    response = Response(response, mimetype='text/csv')
    response.headers.set("Content-Disposition", "attachment", filename="census-data.csv")
    return response

if __name__ == "__main__":
   app.run(host='0.0.0.0', port=5000)
