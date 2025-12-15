from fastapi import FastAPI
from fastapi import UploadFile,File,Form,BackgroundTasks,status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Annotated
import pandas as pd
import io
from dataset_store import normalize_columns,make_table_name,create_table_from_df,insert_data
from db import engine,get_db_session
from charset_normalizer import from_bytes
from model import DatabaseMetadata
import uuid
from infer_metadata import infer_and_store_metadata
import numpy as np
from ai import query_generator
app = FastAPI()

@app.get("/")
async def home_page():
    return "Home Page"

# ******************************************************
# Upload File
# ******************************************************
class GetFile(BaseModel):
    file: UploadFile = Field(...)

@app.post("/api/upload")
async def upload_file(
    payload: Annotated[GetFile, Form()],
    # db: get_db_session
    background_task: BackgroundTasks
    ):
    try:
        if not payload.file.filename.lower().endswith(".csv"):
            return JSONResponse(
                content=({"error":"Only .csv file supported please provide the correct format"}),
                status_code= status.HTTP_400_BAD_REQUEST
            )
        
        content = await payload.file.read()

        #detect the encoding apply while reading file
        detected = from_bytes(content).best()
        print("Detected encoding:", detected.encoding)
        print("Confidence:", detected.chaos)  
        df = pd.read_csv(io.BytesIO(content),encoding=detected.encoding)

        #read file with above encoding
        df.columns = [normalize_columns(c) for c in df.columns]
        df = df.replace({np.nan: None})
        table_name = make_table_name("sales")
        table = create_table_from_df(eng=engine,df=df,table_name=table_name)

        insert_data(table=table,engine=engine,df=df,batch_size=1000)

        metadata = DatabaseMetadata(
            file_name = payload.file.filename,
            table_name = table_name,
            table_metadata = {"status":"processing"}
        )

        db = get_db_session()
        db.add(metadata)
        db.commit()
        db.refresh(metadata)

        if db:
            db.close()
        
        background_task.add_task(infer_and_store_metadata,get_db_session(),metadata.id, payload.file.filename,table_name)

        
    except Exception as e:
        print(e)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content= ({"error":"Internal server error"})
        )
    
    return JSONResponse(
    content= {
        "message":"Data is loaded"
    },
    status_code=status.HTTP_200_OK
    )


# ******************************************************
# Analyse data
# ******************************************************
class Query(BaseModel):
    query: str = Field(...,min_length=3,max_length=500)
    table_name: str = Field(...)

@app.post("/api/analyse")
def answer(payload: Annotated[Query,Form()]):
    print(payload.query)
    result =  query_generator(db=get_db_session(),table_name=payload.table_name,user_query=payload.query)
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=({"message": result})
    )