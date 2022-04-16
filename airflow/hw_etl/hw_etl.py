import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

def basic_data_clean(handle_date):
    """
    clean basic data
      - remove missing data
      - change count values to int e.g. sold, comment_count 1.6萬 => 16000
      - process price value to float ex: $1,234 => 1234
    
    :param handle_date: datetime, which date of data need to clean, default is datetime.now()
    """
    def _count_transfer(col_val):
        """
        - replace 萬 to number
        - transfer str to int
        """
        million_transfer = lambda x: float(x.replace("萬", "")) * 10000 if "萬" in x else x
        output = col_val.str.replace(',', '')
        output = output.apply(million_transfer)
        output = output.astype(int)
        return output

    date_str = handle_date.strftime("%Y-%m-%d")
    engine = create_engine("postgresql://postgres:123qwe@192.168.59.1:5432/crawler")
    df = pd.read_sql(f"SELECT * FROM raw.basic WHERE parse_date::date = '{date_str}' and name != '' and price != ''", con=engine)

    df["comment_count"] = _count_transfer(df["comment_count"])
    df["sold"] = _count_transfer(df["sold"])

    df["price"] = df["price"].str.replace("$", "").str.replace(",", "").str.split("-")
    df["price"] = df["price"].apply(max).astype(float)
    
    df.to_sql(schema="etl", name="basic", con=engine, index=False, if_exists="replace")
        