# A script for getting lots of bulk data from Nomis

`./download-data.sh` gets lots of bulk data from Nomis, and `python add_to_db.py` puts it all in a SQLite database.
(To get more data, remove the second `grep` from download-data.sh)

I've uploaded a small example database, `census-example.db`.

I'm hoping that this approach will be useful for some pre-processing tasks.

Note: counts are currently stored as floating-point numbers...

### Example: get all data for a given place

```
sqlite3 census-example.db -header 'select * from counts left join categories on counts.category_id = categories.rowid left join variables on categories.var_id = variables.rowid where place_id = "E02005276" limit 5;'
```

place\_id|year|category\_id|count|var\_id|val\_name|measurement\_unit|stat\_unit|nomis\_code\_2011|var\_name|population|nomis\_table\_code\_2011
 --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- 
E02005276|2011|1|180.0|1|All categories: Family status (lone parent) by sex and economic activity|Count|Household|KS107EW0001|Lone parent households with dependent children|All lone parent households with dependent children where the lone parent is aged 16 to 74|KS107EW
E02005276|2011|2|57.0|1|Lone parent in part-time employment: Total|Count|Household|KS107EW0002|Lone parent households with dependent children|All lone parent households with dependent children where the lone parent is aged 16 to 74|KS107EW
E02005276|2011|3|90.0|1|Lone parent in full-time employment: Total|Count|Household|KS107EW0003|Lone parent households with dependent children|All lone parent households with dependent children where the lone parent is aged 16 to 74|KS107EW
E02005276|2011|4|33.0|1|Lone parent not in employment: Total|Count|Household|KS107EW0004|Lone parent households with dependent children|All lone parent households with dependent children where the lone parent is aged 16 to 74|KS107EW
E02005276|2011|5|22.0|1|Male lone parent: Total|Count|Household|KS107EW0005|Lone parent households with dependent children|All lone parent households with dependent children where the lone parent is aged 16 to 74|KS107EW

### Example: get all data for a given Nomis column

```
sqlite3 census-example.db -header 'select * from counts left join categories on counts.category_id = categories.rowid left join variables on categories.var_id = variables.rowid where nomis_code_2011 = "KS107EW0001" limit 5;'
```

place\_id|year|category\_id|count|var\_id|val\_name|measurement\_unit|stat\_unit|nomis\_code\_2011|var\_name|population|nomis\_table\_code\_2011
 --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- 
K04000001|2011|1|1662205.0|1|All categories: Family status (lone parent) by sex and economic activity|Count|Household|KS107EW0001|Lone parent households with dependent children|All lone parent households with dependent children where the lone parent is aged 16 to 74|KS107EW
E92000001|2011|1|1564681.0|1|All categories: Family status (lone parent) by sex and economic activity|Count|Household|KS107EW0001|Lone parent households with dependent children|All lone parent households with dependent children where the lone parent is aged 16 to 74|KS107EW
W92000004|2011|1|97524.0|1|All categories: Family status (lone parent) by sex and economic activity|Count|Household|KS107EW0001|Lone parent households with dependent children|All lone parent households with dependent children where the lone parent is aged 16 to 74|KS107EW
E12000001|2011|1|90549.0|1|All categories: Family status (lone parent) by sex and economic activity|Count|Household|KS107EW0001|Lone parent households with dependent children|All lone parent households with dependent children where the lone parent is aged 16 to 74|KS107EW
E12000002|2011|1|241336.0|1|All categories: Family status (lone parent) by sex and economic activity|Count|Household|KS107EW0001|Lone parent households with dependent children|All lone parent households with dependent children where the lone parent is aged 16 to 74|KS107EW
