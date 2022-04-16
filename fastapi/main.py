from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
import urllib
from sqlalchemy import create_engine
import pandas as pd

conn = "postgresql://postgres:123qwe@192.168.59.1:5432/crawler"
app = FastAPI()

class PostItem(BaseModel):
    url: str
    search: Optional[bool] = True


@app.get("/api/crawl_url")
def read_crawl_url_with_type(search: Optional[str]=None):
    """
    Get option crawl url
    :param search: str, None, search, exact
    """
    print(search)
    
    query = """SELECT url FROM config.crawl_url"""
    if search == "search":
        query += f" WHERE search = true"
    elif search == "exact":
        query += f" WHERE search = false"

    engine = create_engine(conn)
    df = pd.read_sql(query, con=engine)

    return df["url"].to_list()


@app.post("/api/crawl_url")
def insert_crawl_url(item: PostItem):
    """
    insert new crawl url
    """

    if "https://shopee.tw" not in item.url:
        item.url = f"https://shopee.tw/search?keyword={item.url}"
        item.search = True
    elif "https://shopee.tw/search?ketword" in item.url:
        item.search = True
    df = pd.DataFrame({"url": [item.url], "search": [item.search]})
    print(df)
    engine = create_engine(conn)
    df.to_sql(name="crawl_url", schema="config", con=engine, if_exists="append", index=False)

    return "item.url is inserted into the list"


@app.delete("/api/crawl_url")
def delete_crawl_url(url: str):
    query = """SELECT * FROM config.crawl_url"""

    engine = create_engine(conn)
    df = pd.read_sql(query, con=engine)

    del_count = sum(df["url"] == url)
    df = df.loc[df["url"] != url]
    df.to_sql(name="crawl_url", schema="config", con=engine, if_exists="replace", index=False)

    return f"{del_count} url are deleted"


@app.get("/api/basic")
def read_crawl_url(url: Optional[str]=None, name: Optional[str]=None, seller: Optional[str]=None):
    """
    Get basic data
    """
    query = """SELECT * FROM etl.basic"""
    where_query = []
    if url != None:
        where_query.append(f"url = '{url}'")
    if name != None:
        where_query.append(f"name like '%{name}%'")
    if seller != None:
        where_query.append(f"seller = '{seller}'")
    where_query = " AND ".join(where_query)
    where_query = " WHERE " + where_query
    query += where_query
    print(query)

    engine = create_engine(conn)
    df = pd.read_sql(query, con=engine)

    return df.to_json(orient="records")


@app.get("/api/comment")
def read_crawl_url(url: Optional[str]=None, seller: Optional[str]=None):
    """
    Get all basic data
    """
    query = """SELECT * FROM raw.comment"""
    where_query = []
    if url != None:
        where_query.append(f"url = '{url}'")
    if seller != None:
        where_query.append(f"seller = '{seller}'")
    where_query = " AND ".join(where_query)
    where_query = " WHERE " + where_query
    query += where_query
    print(query)

    engine = create_engine(conn)
    df = pd.read_sql(query, con=engine)

    return df.to_json(orient="records")