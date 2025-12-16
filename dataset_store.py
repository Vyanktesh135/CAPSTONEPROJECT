import re
import uuid
import pandas as pd
from sqlalchemy import MetaData, Table, Column, Text,Column, Text, Numeric,Date,Boolean
from sqlalchemy.engine import Engine
from infer_metadata import infer_col_type

#Normalize the columns
def normalize_columns(col:str) ->str:
    col = col.strip().lower()
    col = re.sub(r"[^a-z0-9]+","_",col)
    col = re.sub(r"_+","_",col).strip("_")
    if not col:
        col = "col"
    return col

#Generate the suitable table name
def make_table_name(prefix:str = "dataset") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"

#Create Table
def create_table_from_df(eng: Engine,table_name:str,schema:dict) -> Table:
    print("engine dialect: ",eng.dialect.name)
    print("engine url: ",eng.url)
    md = MetaData()
    data_type_dict = {
        "string": Text,
        "boolean": Boolean,
        "numeric": Numeric,
        "date": Date
    }
    cols = [Column("__id",Text,primary_key=True)]
    for key,value in schema.items():
        cols.append(Column(key,data_type_dict[value],nullable=True))
    
    table = Table(table_name,md,*cols)
    md.create_all(eng)

    return table

#Insert Into Table
def insert_data(engine: Engine, table: Table, df: pd.DataFrame,batch_size:int = 1000):
    print("Data Insertion is started ..!")
    df2 = df.copy()

    df2["__id"] = [uuid.uuid4().hex[:12] for _ in range(len(df2))]

    records = df2.to_dict(orient="records")

    with engine.begin() as conn:
        for i in range(0,len(records),batch_size):
            conn.execute(table.insert(),records[i:i+batch_size])
