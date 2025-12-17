from langchain_openai import ChatOpenAI
from config import settings
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel,field_validator
from typing import Tuple,Optional,List,Union,Dict
from model import DatabaseMetadata
import json
from enum import Enum
from agents import function_tool
from agents import Agent,Runner,SQLiteSession
from braintrust import init_logger, load_prompt
from braintrust.wrappers.openai import BraintrustTracingProcessor
from db import get_db_session
from sqlalchemy import text
from sqlalchemy.orm import session
from fastapi.encoders import jsonable_encoder

llm = ChatOpenAI(model = "gpt-5.1",api_key=settings.OPENAI_API_KEY,temperature=0)

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
    date: Optional[Tuple[str, str]] = None

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

class OrderByAD(str,Enum):
    asc = "ASC"
    desc = "DESC"

class OrderBy(BaseModel):
    funtion: Optional[AggFunc] = None
    column_name: str
    order_by: OrderByAD

class Answer(BaseModel):
    filters: Filters
    extra_filter: List[Extrafilter]
    metrics: List[Dict[AggFunc,str]]
    group_by: List[str]
    order_by: List[OrderBy]
    limit: int
    notes: List[str]

class ResultData(BaseModel):
    answer: Answer

class c_violations(str,Enum):
    rule: str
    detail: str

class SQL_Validator(BaseModel):
    verdict: str
    reason: str | None
    violations: Optional[List[c_violations]] = None
    suggested_fix: str | None

def run_sql(query:str,db:session = get_db_session()):
    try:
        data = []
        for row in db.execute(text(query)).mappings():
            data.append(row)
        return data
    except Exception as e:
        print("Failed to run query ",e)
        raise Exception("Failed to run query ",e)
    finally:
        if db:
            db.close()
            print("Session is closed .")

def sql_builder ( db_result,table_name,data:dict):
    try:
        print("Input Data:",data)
        column_mapping = db_result["column_mapping"]
        columns = {item["name"] for item in db_result["columns"]}

        select_part = ""
        where_part = "WHERE 1=1"
        group_by_part = ""
        order_by_part = ""
        limit = None

        if data["metrics"]:
            select_part = ""
            for i in data["metrics"]:
                for key in i.keys():
                    if select_part:
                        print("Im in SQL script")
                        select_part += f" , {key.value}("
                    else:
                        select_part += f"{key.value}("
                    
                    if i[key] in column_mapping.keys():
                        select_part += f"{column_mapping[i[key]]})"
                    else:
                        select_part += f"{i[key]})"
        
        if data["filters"]:
            filters = data["filters"]
            for filter in filters:
                if filters[filter] and filter != 'date':
                    value = column_mapping[filter]
                    values = ", ".join(f"'{str(item)}'" for item in filters[filter])
                    where_part += f" AND {value} IN ({values})"
                
                if filters[filter] and filter == 'date':
                    value = column_mapping[filter]
                    values = " AND ".join(f"'{str(item)}'" for item in filters[filter])
                    where_part += f" AND {value} Between {values}"
                                
        if data["extra_filter"]:
            for item in data["extra_filter"]:
                if item["column"] in columns:
                    if item["op"] in ("IS NULL", "IS NOT NULL"):
                        where_part += f" AND {item["column"]} {item["op"].value}"
                    elif item["op"] in ("BETWEEN", "NOT BETWEEN"):
                        where_part += f" AND {item["column"]} {item["op"].value} '{item["value"][0]}' AND '{item["value"][1]}'"
                    elif item["op"] in ("IN", "NOT IN"):
                        values = ", ".join(f"'{v}'" for v in item["value"])
                        where_part += f" AND {item["column"]} {item["op"].value} ({values})"                        
                    else:
                        where_part += f" AND {item["column"]} {item["op"].value} '{item["value"]}'"
        
        if data["group_by"]:
            for i in data["group_by"]:
                value = i
                if i in column_mapping.keys():
                    value = column_mapping[i]
                if group_by_part:
                     group_by_part += f", {value}"
                else:
                     group_by_part += value

        if data["order_by"]:
            for item in data["order_by"]:
                column_name = column_mapping[item["column_name"]] if item["column_name"] in column_mapping.keys() else item["column_name"]
                if item["funtion"]:
                    function_name = f"{item["funtion"].value}("if item["funtion"] else ""
                    column_name = function_name + column_name + ")"
                
                ob = item["order_by"].value
                if order_by_part:
                    order_by_part += f", {column_name} {ob}"
                else:
                    order_by_part += f"{column_name} {ob}"

        if data["limit"]:
            limit = data["limit"]  
        print(select_part)
        if group_by_part:
            select_part = group_by_part + "," + select_part
        
        final_sql = f"""select {select_part} from {table_name}
        {f"{where_part}" if where_part else ''}
        {f"group by {group_by_part}" if group_by_part else ''}
        {f"order by {order_by_part}" if order_by_part else ''}
        {f"limit {limit}" if limit else ''}
        """

        final_sql.replace('None','null')
        print("\n\nSQL Builder : ",final_sql,"\n\n")
        return final_sql
    except Exception as e:
        print(e)
        raise Exception("Failed to generate the SQL",e)

def query_generator(table_name,user_query,db:session = get_db_session()):
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
        10) Order by allowed:
            a) region, item_type, channel, date, quarter (only if date role exists) or 
            b) column from column_catalog alone or 
            c) Aggregate function with region, item_type, channel, date, quarter (only if date role exists) or column from column_catalog alone.
            order by format (if applicable Aggregate function name,column_name,Ascending (asc) / Descending(desc))
        11) Limit should only contain numeric value.
        12) All date fields or entity in response must be in format like date_range [YYYY-01-01, YYYY-12-31] 
            - if response contains year alone convert to ate_range [YYYY-01-01, YYYY-12-31]
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

        output_query = sql_builder(data=llm_response.model_dump()['answer'],db_result = result,table_name=table_name)
        return { "llm_response":llm_response.model_dump(),"sql_query":output_query}
    except Exception as e:
        print("Failed while generating query : ",e)
        raise Exception("Failed while generating query : ",e)
    finally:
        if db:
            db.close()
            print("Session is closed.")

def query_validator(TABLE_NAME,COLUMN_CATALOG_JSON,SQL_QUERY):
    try:
        SYSTEM_PROMPT = """You are a SQL Safety & Correctness Validator for a Postgres analytics system.

        Your task: validate a generated SQL query against strict rules and return a verdict.

        Rules the SQL MUST satisfy:
        1) It must be a single SELECT query only.
        - Disallow: INSERT, UPDATE, DELETE, UPSERT, MERGE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE, COPY, CALL, DO, EXECUTE, SET, WITH ... INSERT/UPDATE/DELETE, multiple statements separated by ';'.
        2) It must reference exactly ONE table, and that table must be exactly the provided table_name.
        - No other tables, schemas, views, CTEs that introduce other relations, joins, subqueries that read from other tables.
        3) It may only use columns that appear in the provided column_catalog.
        4) Date operations must be safe:
        - If casting text to date, use "::date" or CAST(... AS date) only on allowed date columns.
        - BETWEEN boundaries must use placeholders and explicit ::date casts are preferred.
        5) Numeric operations must be safe:
        - If casting text to numeric, use "::numeric" or CAST(... AS numeric) only on allowed numeric columns.
        - Use NULLIF in division denominators to avoid division by zero.
        6) The query must be syntactically plausible Postgres SQL.

        You must return ONLY valid JSON, no markdown, no extra text.

        If the query is correct:
        - verdict = "correct"
        - reasons should include a short confirmation.
        - violations should be []
        - suggested_fix must be null

        If the query is wrong:
        - verdict = "wrong"
        - include clear reasons and violations
        - suggested_fix: provide a corrected SELECT query only IF you can fix it without inventing columns/tables and while preserving placeholders. Otherwise set suggested_fix to null.
        """

        USER_PROMPT = """Validate the following SQL against the rules.

        table_name: {TABLE_NAME}

        column_catalog (allowed columns):
        {COLUMN_CATALOG_JSON}

        SQL to validate:
        {SQL_QUERY}

        {format_instruction}
        """

        analysis_parser = PydanticOutputParser(pydantic_object=SQL_Validator)
        message = (
            ChatPromptTemplate.from_messages([
                ("system",SYSTEM_PROMPT),
                ("user",USER_PROMPT)
            ]).partial(format_instruction = analysis_parser.get_format_instructions())
        )

        chain = message | llm | analysis_parser

        verdict_response: SQL_Validator = chain.invoke({
            "TABLE_NAME":TABLE_NAME,
            "COLUMN_CATALOG_JSON": COLUMN_CATALOG_JSON,
            "SQL_QUERY": SQL_QUERY
        })

        print(verdict_response.model_dump_json())
        return verdict_response
    except Exception as e:
        print("Failed to valiate ",e)
        raise Exception("Failed to valiate ",e)

def result_generator(query_generator_result,USER_QUESTION,QUERY_RESULT_ROWS):
    try:
        print("Generating the result ..")
        SYSTEM_PROMPT = """You are an analytics result explanation assistant.
        Your task:
        - Explain the results of a data query to a business user.
        - Use ONLY the provided query plan, executed SQL results, and notes.
        - Do NOT invent numbers, trends, or interpretations beyond the data.
        - Do NOT mention SQL, databases, tables, or implementation details.
        - Keep the explanation clear, concise, and business-friendly.

        Rules:
        1) Use the user's original question as context for your explanation.
        2) Base all statements strictly on the provided query result rows.
        3) If the result is empty, clearly say that no matching data was found.
        4) Clearly state which filters were applied.
        5) If notes are provided (e.g., missing fields, ignored filters), surface them politely.
        6) If metrics include multiple values, explain each briefly.
        7) Do NOT speculate or infer causes unless explicitly supported by the data.

        Output format:
        - A short direct answer (1-3 sentences)
        - A bullet list summarizing applied filters and metrics
        - Optional notes (if any)

        Return ONLY plain text. No JSON. No markdown.
        """
        
        USER_PROMPT = """User Question:
        "{USER_QUESTION}"

        Query Plan (validated):
        Filters:
        - Region: {REGION_FILTER}
        - Item Type: {ITEM_TYPE_FILTER}
        - Channel: {CHANNEL_FILTER}
        - Date Range: {DATE_RANGE}

        Extra Filters:
        {EXTRA_FILTERS_SUMMARY}

        Metrics Requested:
        {METRICS_LIST}

        Group By:
        {GROUP_BY_LIST}

        Query Result Rows:
        {QUERY_RESULT_ROWS}

        Notes:
        {NOTES_LIST}
        """

        message = (
            ChatPromptTemplate([
                ("system",SYSTEM_PROMPT),
                ("human",USER_PROMPT)
            ])
        )

        chain = message | llm

        result = chain.invoke({
            "USER_QUESTION":USER_QUESTION,
            "REGION_FILTER":json.dumps(query_generator_result["filters"]["region"]),
            "ITEM_TYPE_FILTER":json.dumps(query_generator_result["filters"]["item_type"]),
            "CHANNEL_FILTER":json.dumps(query_generator_result["filters"]["channel"]),
            "DATE_RANGE":json.dumps(query_generator_result["filters"]["date"]),
            "EXTRA_FILTERS_SUMMARY":json.dumps(query_generator_result["extra_filter"]),
            "METRICS_LIST":json.dumps(query_generator_result["metrics"]),
            "GROUP_BY_LIST": json.dumps(query_generator_result["group_by"]),
            "NOTES_LIST": json.dumps(query_generator_result["notes"]),
            "QUERY_RESULT_ROWS": QUERY_RESULT_ROWS
        })

        print(result.model_dump_json())
        return json.loads(result.model_dump_json())
    except Exception as e:
        print("Failed to generate the final answer ..!",e)
        raise Exception("Failed to generate the final answer ..!",e)

def orchestrator(table_name,user_query,db:session = get_db_session()):
    try:
        print(table_name,user_query)
        generated_json = query_generator(table_name=table_name,user_query=user_query) 
        
        result = db.query(DatabaseMetadata.table_metadata).filter(DatabaseMetadata.table_name == table_name).first()
        column_type_mapping = []
        for table_metadata in result:
            for i in table_metadata["columns"]:
                column_type_mapping.append({"column_name":i["name"],"type":i["type"]})

        sql_validation = query_validator(TABLE_NAME=table_name,COLUMN_CATALOG_JSON=json.dumps(column_type_mapping),SQL_QUERY=generated_json["sql_query"])

        verdict = sql_validation.verdict.lower()
        llm_nlp = None
        if verdict == "correct":
            final_result = run_sql(generated_json["sql_query"])
            final_result = [dict(row) for row in final_result]
            print("Result Generation Started ..")
            llm_nlp = result_generator(query_generator_result = generated_json["llm_response"]["answer"],USER_QUESTION = user_query,QUERY_RESULT_ROWS = jsonable_encoder(final_result))
            return llm_nlp["content"]
        else:
            # TODO
            # call the SQL Fixing Agent
            print("Hii")
            return "Invalid Query Generated"
        
    except Exception as e:
        print("Failed ..",e)
        raise Exception("Failed to analyze",e)