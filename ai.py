from langchain_openai import ChatOpenAI
from config import settings
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel,field_validator
from typing import Tuple,Optional,List,Union,Dict
from model import DatabaseMetadata
import json
from enum import Enum

class SQLOperator(str, Enum):
    eq = "="
    ne = "!="
    neq = "<>"
    gt = ">"
    gte = ">="
    lt = "<"
    lte = "<="

    in_ = "IN"
    not_in = "NOT IN"

    between = "BETWEEN"
    not_between = "NOT BETWEEN"

    like = "LIKE"
    not_like = "NOT LIKE"
    ilike = "ILIKE"

    is_null = "IS NULL"
    is_not_null = "IS NOT NULL"

class Filters(BaseModel):
    region: Optional[List[str]] = None
    item_type: Optional[List[str]] = None
    channel: Optional[List[str]] = None
    date_range: Optional[Tuple[str, str]] = None

class Extrafilter(BaseModel):
    column: Optional[str] = None
    value: Optional[Union[str, int, float, List, Tuple]] = None
    op: SQLOperator

    @field_validator("value",mode="before")
    @classmethod
    def validate_value(cls,v,info):
        op = info.data.get("op")

        if op in {SQLOperator.is_null , SQLOperator.is_not_null}:
            return None
        
        if op in {SQLOperator.in_, SQLOperator.not_in}:
            if not isinstance(v,list):
                raise ValueError("IN / NOT IN requires a list value")

        if op in {SQLOperator.between , SQLOperator.not_between}:
            if not isinstance(v,(list,tuple)) or len(v) != 2:
                raise ValueError("BETWEEN requires exactly two values")
        
        return v

class AggFunc(str,Enum):
    sum = "SUM"
    min = "MIN"
    max = "MAX"
    avg = "AVG"
    count = "COUNT"
    count_distinct = "COUNT_DISTINCT"
    
class Answer(BaseModel):
    filters: Filters
    extra_filter: List[Extrafilter]
    metrics: List[Dict[AggFunc,str]]
    group_by: List[str]
    notes: List[str]

class ResultData(BaseModel):
    answer: Answer

def sql_builder ( db_result,table_name,data:dict):
    try:
        print("Input Data:",data)
        column_mapping = db_result["column_mapping"]
        columns = {item["name"] for item in db_result["columns"]}

        select_part = ""
        where_part = "WHERE 1=1"
        group_by_part = ""

        if data["metrics"]:
            select_part = ""
            for i in data["metrics"]:
                for key in i.keys():
                    if select_part:
                        print("Im in SQL script")
                        select_part += f" AND {key}("
                    else:
                        select_part += f"{key}("
                    
                    if i[key] in column_mapping.keys():
                        select_part += f"{column_mapping[i[key]]})"
                    else:
                        select_part += f"{i[key]})"
        
        if data["filters"]:
            filters = data["filters"]
            for filter in filters:
                if filters[filter] and (filter in column_mapping.keys()):
                    value = column_mapping[filter]
                    values = ", ".join(f"'{str(item)}'" for item in filters[filter])
                    where_part += f" AND {value} IN ({values})"
                
        if data["extra_filter"]:
            for item in data["extra_filter"]:
                if item["column"] in columns:
                    if item["op"] in ("IS NULL", "IS NOT NULL"):
                        where_part += f" AND {item["column"]} {item["op"]}"
                    elif item["op"] in ("BETWEEN", "NOT BETWEEN"):
                        where_part += f" AND {item["column"]} {item["op"]} '{item["value"][0]}' AND '{item["value"][1]}'"
                    elif item["op"] in ("IN", "NOT IN"):
                        values = ", ".join(f"'{v}'" for v in item["value"])
                        where_part += f" AND {item["column"]} {item["op"]} ({values})"                        
                    else:
                        where_part += f" AND {item["column"]} {item["op"]} '{item["value"]}'"
        
        if data["group_by"]:
            for i in data["group_by"]:
                value = i
                if i in column_mapping.keys():
                    value = column_mapping[i]
                if group_by_part:
                     group_by_part += f", {value}"
                else:
                     group_by_part += value

        final_sql = f"""select {select_part} from {table_name}
        {f"where {where_part}" if where_part else ''}
        {f"group by {group_by_part}" if group_by_part else ''}
        """

        final_sql.replace('None','null')
        print(final_sql)

        return final_sql
    except Exception as e:
        print(e)
        raise Exception("Failed to generate the SQL",e)


def query_generator(db,table_name,user_query):
    try:
        data = db.query(DatabaseMetadata).filter(DatabaseMetadata.table_name == table_name).first()

        if data.table_metadata["status"] != "ready":
            return {"message":"Data processing still in progress please wait for sometime..!"}
        
        SYSTEM_PROMPT = """You are a data analytics query planner for a merchandising sales dataset.
        Your job: convert the user's natural-language question into a JSON query plan that can be executed deterministically with SQL.

        Rules you MUST follow:
        1) Output ONLY valid JSON. No markdown, no comments, no extra text.
        2) Use ONLY the supported roles, column_mapping for primary filter AND column_catalog for any extra filters/groupingt.
        3) Do NOT invent columns or values outside column_catalog.
        4) Users may request aggregation functions like sum/total, average/mean, min/max, count.
            Map synonyms in result metrics:
            a. If user mention total or sum then use aggregate function "SUM"
            b. If user mention average or avg or mean then use aggregate function "AVG"
            c. If user mention number of or how many then use aggregate function "COUNT" 
            d. If user mention distinct number of or unique number of then use aggregate function "COUNT_DISTINCT"
            e. If user ask small or min or minimum then use aggregate function "MIN"
            f. If user ask big or max or maximum then use aggregate function "MAX"
        5) If the user asks for something the dataset cannot support (e.g., sales channel when channel column is null, or a region/item not in allowed values), set that filter to null and add an explanation to "notes".
        6) Always normalize region/item/channel values to EXACT strings from allowed values.
        7) For time:
        - If the user specifies a year, convert to date_range [YYYY-01-01, YYYY-12-31]
        - If the user specifies a quarter (Q1..Q4) and year, convert to correct date_range
        - If the user specifies a date range, use it directly (YYYY-MM-DD)
        - If time is missing, set date_range to null and add a note.
        8) Metrics allowed: revenue, units_sold, avg_selling_price or column from column_catalog alone.
        9) Group_by allowed: region, item_type, channel, date, quarter (only if date role exists) or column from column_catalog alone.

        Your output MUST be valid JSON that conforms exactly to the provided schema.
        Do not include any text outside the JSON.
        """
        result = data.table_metadata
        USER_QUESTION = user_query
        COLUMN_MAPPING_JSON = json.dumps(result['column_mapping'])
        REGIONS_LIST = json.dumps(result['distinct_values']['regions'])
        ITEM_TYPES_LIST =  json.dumps(result['distinct_values']['item_types'])
        CHANNELS_LIST = json.dumps(result['distinct_values']['channels'])
        COLUMN_CATALOG = json.dumps(result['columns'])
        USER_PROMPT = """
        -- Details --
        {{
        "question": "{USER_QUESTION}",
        "supported_roles": ["region","item_type","channel","date","units_sold","revenue","avg_selling_price"],
        "column_mapping": {COLUMN_MAPPING_JSON},
        "column_catalog": {COLUMN_CATALOG}
        "allowed_values": {{
            "regions": {REGIONS_LIST},
            "item_types": {ITEM_TYPES_LIST},
            "channels": {CHANNELS_LIST}
        }},
        "defaults": {{
            "metrics_if_unspecified": ["revenue","units_sold","avg_selling_price"]
        }}
        }}

        {format_instruction}
        """

        llm = ChatOpenAI(model = "gpt-5.1",api_key=settings.OPENAI_API_KEY,temperature=0)
        analysis_parser = PydanticOutputParser(pydantic_object=ResultData)
        message = (
        ChatPromptTemplate.from_messages(
                [
                    ("system",SYSTEM_PROMPT),
                    ("human",USER_PROMPT)
                ]
            ).partial(format_instruction = analysis_parser.get_format_instructions())
        )

        chain = message | llm | analysis_parser

        llm_response: ResultData = chain.invoke({
            "USER_QUESTION":USER_QUESTION,
            "COLUMN_MAPPING_JSON": COLUMN_MAPPING_JSON,
            "COLUMN_CATALOG": COLUMN_CATALOG,
            "REGIONS_LIST": REGIONS_LIST,
            "ITEM_TYPES_LIST": ITEM_TYPES_LIST,
            "CHANNELS_LIST": CHANNELS_LIST
        })

        print(llm_response)

        sql_builder(data=llm_response.model_dump()['answer'],db_result = result,table_name=table_name)
        return llm_response.model_dump()
    except Exception as e:
        print("Failed while generating query : ",e)
        raise Exception("Failed while generating query : ",e)

