import os

from dotenv import load_dotenv
from snowflake.connector import connect

load_dotenv()


ACCOUNT = os.getenv("account")
USER = os.getenv("user")
PASSWORD = os.getenv("password")
ROLE = os.getenv("role")
WAREHOUSE = os.getenv("warehouse")
DATABASE = os.getenv("database")
SCHEMA = os.getenv("schema")


def simple_query():
    """ """
    try:
        with connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA,
        ) as conn:
            with conn.cursor() as cs:
                print(cs.execute("select 1+1").fetchall())
                print(
                    cs.execute(
                        "select A.*, B.* FROM TPCH_SF1.ORDERS A JOIN TPCH_SF1.CUSTOMER B ON A.O_CUSTKEY = B.C_CUSTKEY WHERE A.O_ORDERDATE BETWEEN '1992-12-01' and '1992-12-31' ORDER BY A.O_ORDERDATE DESC"
                    ).fetchall()
                )
    except Exception as err:
        print(f"Snowflake connection error : {err}")


def main():
    """ """
    print(f"Account : {ACCOUNT}")
    simple_query()


if __name__ == "__main__":
    main()
