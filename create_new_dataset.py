from re import T
from datasets import arrow_reader, arrow_writer
import json
import numpy as np
import pyarrow as pa

import os

print(os.getcwd())

table = arrow_reader.ArrowReader.read_table("../../../transformers_cache/spider/spider/1.0.0/3c571ccbabdc104a8d1f14edff4ce10d09d7724b0c3665fc42bec1ed51c84bf3/spider-validation.arrow")

print(table["query"][0])

pd = table.to_pandas()

print(pd.dtypes)

newdata = json.load(open("../../../spider-schema-gnn-global/spider/dev_spider_processed_sqlmetadata.json", "rb"))

# db_column_names
# db_column_types
# db_table_names


for i, data in enumerate(newdata):
    new_tables = []
    new_table_indices = []
    new_column_types = []

    check_table = pd["db_table_names"][i]
    check_column = pd['db_column_names'][i]

    assert len(data["tables"])==len(pd["db_table_names"][i])
    #assert (data["tables"] == pd["db_table_names"][i])
    for j, isValid in enumerate(data["gt_tables"]):
        if isValid:
            new_tables.append(data["tables"][j])
            new_table_indices.append(j)
    new_column_indices = np.array(pd['db_column_names'][i]["table_id"])

    new_column_indices2 = np.argwhere(new_column_indices==new_table_indices[0])
    for k in range(1,len(new_table_indices)):
        new_column_indices2 = np.vstack((new_column_indices2, np.argwhere(new_column_indices==new_table_indices[k])))

    if pd['db_column_names'][i]["table_id"][0] == -1:
        new_column_indices2 = np.vstack((np.array([[0]]), new_column_indices2))
        new_table_indices.append(-1)
    new_column_indices2 = new_column_indices2.reshape(-1,)

    new_columns = np.take(pd['db_column_names'][i]["column_name"], new_column_indices2.tolist())
    new_column_types = np.take(pd['db_column_types'][i], new_column_indices2.tolist())

    #ew_table = np.select(pd['db_column_names'][i]["table_id"]==new_table_indices, pd['db_column_names'][i]["table_id"])
    
    #print(pd['db_column_names'][i]["table_id"])
    #print(new_table_indices)
    #print(np.in1d(pd['db_column_names'][i]["table_id"], new_table_indices))

    relevant_indices = pd['db_column_names'][i]["table_id"][np.in1d(pd['db_column_names'][i]["table_id"], new_table_indices)]
    new_table_indices = []
    index = 0
    if relevant_indices[index] == -1:
        new_table_indices.append(-1)
        index+=1
    value = 0
    while index<len(relevant_indices):
        if index>0 and relevant_indices[index-1]!=-1 and relevant_indices[index-1]!=relevant_indices[index]:
            value+=1
        new_table_indices.append(value)
        index+=1

    pd["db_table_names"][i] = new_tables
    pd['db_column_names'][i]["table_id"] = new_table_indices
    pd['db_column_names'][i]["column_name"] = new_columns
    pd['db_column_types'][i] = new_column_types
    pd["db_primary_keys"][i]["column_id"] = np.where(np.in1d(new_column_indices2, pd["db_primary_keys"][i]["column_id"]))[0]
    pd["db_foreign_keys"][i]["column_id"] = np.where(np.in1d(new_column_indices2, pd["db_foreign_keys"][i]["column_id"]))[0]
    pd["db_foreign_keys"][i]["other_column_id"] = np.where(np.in1d(new_column_indices2, pd["db_foreign_keys"][i]["other_column_id"]))[0]

print("here i am")
len(pd)
#pd.to_pickle("../../data/validation_relevanttables_output.pkl")
pd.to_csv("../../data/validation_relevanttables_output.csv")

# for i in range(len(pd)):
#     print(type(pa.array(pd['db_column_names'][i]["table_id"], type=pa.ListScalar)))
#     #for j in pd['db_column_names'][i]["table_id"]:
#     print(type(table['db_column_names'][i]['table_id']))    
#     #for j in table['db_column_names'][i]['table_id']:
#     #    print(type(j))
#     table['db_column_names'][i]['table_id'] = pa.array(pd['db_column_names'][i]["table_id"])
#     print(type(table['db_column_names'][i]['table_id']))
#     print(type(table['db_column_names'][i]["column_name"]))
#     print(type(table['db_column_types'][i]))
#     print(type(table["db_primary_keys"][i]["column_id"]))
#     print(type(table["db_foreign_keys"][i]["column_id"]))
#     print(type(table["db_foreign_keys"][i]["other_column_id"]))

# schema = table.schema
# table = pa.Table.from_pandas(pd)
# #pa.Schema.from_pandas(pd)

# writer = pa.ipc.new_file("relevanttables_train_spider.arrow", schema)
# writer.write(table)
# writer.close()

#arrow_writer.ArrowWriter.write_table(pa.Table.from_pandas(pd), "relevanttables_train_spider.arrow")
    #for j, table in enumerate(new_tables):

    #new_columns.append(data["columns"][j])
    #new_column_types.append(data["column_types"][j])

    #print(pd['db_table_names'][i])
    #print(pd['db_column_names'][i])
    #print(pd['db_column_types'][i])
    #print(data["tables"])
    #print(data["gt_tables"])

