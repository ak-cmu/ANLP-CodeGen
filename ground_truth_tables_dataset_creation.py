import sqlite3
from sql_metadata import Parser
import json

# question | table column names | labels

dataset = []

f = json.load(open("train_spider.json", "rb"))
count = 0
for ele in f:
    item = {}
    item["question"] = ele["question"]
    item["db_id"] = ele["db_id"]
    con = sqlite3.connect("database/"+item["db_id"]+"/"+item["db_id"]+".sqlite")
    con.text_factory = lambda b: b.decode(errors = 'ignore')
    cur = con.cursor()
    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    item["tables"] = [table[0] for table in tables]
    item["columns"] = []
    item["column_types"] = []
    item["values"] = []
    for table in item["tables"]:
        column_names = []
        column_types = []
        value_examples = []
        for column in cur.execute("PRAGMA table_info("+table+");").fetchall():
            column_names.append(column[1])
            values = cur.execute("SELECT \""+column[1]+"\" FROM "+table+";").fetchall()
            if len(values)>=1 and len(values[0])>=1:
                value_examples.append(values[0][0])
            else:
                value_examples.append(None)
            column_types.append(column[2])
        item["columns"].append(column_names)
        item["column_types"].append(column_types)
        item["values"].append(value_examples)
    
    item["gt_tables"] = []
    tables = Parser(ele["query"]).tables
    tables = [table.lower() for table in tables]
    for table in item["tables"]:
        if table.lower() in tables:
            item["gt_tables"].append(True)
        else:
            item["gt_tables"].append(False)
    assert len(item["gt_tables"]) == len(item["tables"])
    assert len(item["columns"]) == len(item["tables"])
    dataset.append(item)
    count+=1

print(count)

json.dump(dataset, open("train_spider_processed_sqlmetadata.json", "w"))


    
