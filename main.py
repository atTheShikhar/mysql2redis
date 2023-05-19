import json
import time
from models import RedisDocument, Table, Creds
from redis_actions import RedisActions
import mysql.connector, csv


cr = Creds()
rdb = RedisActions(host=cr.redis.host, password=cr.redis.pwd)


sql_conn = mysql.connector.connect(
    user=cr.mysql.user,
    password=cr.mysql.pwd,
    host=cr.mysql.host,
    db=cr.mysql.db,
)
sql_conn.autocommit = True
sql_cursor = sql_conn.cursor(dictionary=True)


def persist_connection(func):
    def inner(*args, **kwargs):
        global sql_conn
        global sql_cursor
        if not sql_conn.is_connected():
            sql_conn = mysql.connector.connect(
                user=cr.mysql.user,
                password=cr.mysql.pwd,
                host=cr.mysql.host,
                db=cr.mysql.db,
            )
            sql_conn.autocommit = True
            sql_cursor = sql_conn.cursor(dictionary=True)
        return func(*args, **kwargs)
    return inner


def get_tables() -> list[Table]:
    """
    Desc: Gets table metadata which are to be migrated.
    """
    tables = []
    with open("tables.csv") as tfile:
        cf = csv.DictReader(tfile, doublequote=True, skipinitialspace=True, lineterminator="\n")
        for l in cf:
            tables.append(Table(**l))
    return tables


@persist_connection
def extract_data(table, pk, limit, offset):
    """
    Desc: Fetches the data from MySQL table.
    """
    sql_cursor.execute(f'''
        SELECT * 
        FROM `{table}` 
        ORDER BY {pk} ASC 
        LIMIT {limit}
        OFFSET {offset}
    ''')
    rows = sql_cursor.fetchall()
    return rows


def fmt_mysql2redis(table_meta: Table, table_data: list[dict]) -> list[RedisDocument]:
    docs = []
    for td in table_data:
        doc = RedisDocument(
            key=f"{table_meta.redis_prefix}:{td[table_meta.mysql_pk]}", 
            val=td
        )
        docs.append(doc)
    return docs


def load_data(docs: list[RedisDocument]):
    """
    Desc: Loads the data to Redis
    """
    rdb.pset_keys_json(docs)
    return


class MigrationVars():
    def __init__(self) -> None:
        self.DEFAULT_PAGE = 1
        self.DEFAULT_LIMIT = 100
        self.QUERY_SLEEP_SEC = 1

    def reset(self):
        self.page = self.DEFAULT_PAGE
        self.limit = self.DEFAULT_LIMIT
        self.sleep_duration = self.QUERY_SLEEP_SEC
        self.rowcount = 0
        self.done = False

    def get_offset(self):
        offset = (self.page - 1) * self.limit
        return offset

    def next_page(self, inserted_rowcount: int):
        self.rowcount += inserted_rowcount
        self.page += 1

    def table_done(self):
        self.done = True

hr = "\n============================================\n" # horizontal rule

def migrate_db(tables: list[Table]):
    mvars = MigrationVars()

    for t in tables:
        mvars.reset()
        print(f"{hr}MIGRATING TABLE: {t.mysql_table}")
        while not mvars.done:
            offset = mvars.get_offset()
            rows = extract_data(t.mysql_table, t.mysql_pk, mvars.limit, offset)
            if rows == []:
                mvars.table_done()
                print(f"ROWS MIGRATED: {mvars.rowcount}{hr}")
            else:
                load_data(fmt_mysql2redis(t, rows))
                mvars.next_page(inserted_rowcount=len(rows))
            time.sleep(mvars.sleep_duration)

def index_redis_docs(tables: list[Table]):
    with open("redis_index.json") as f:
        index_data: dict = json.load(f)

    for t in tables:
        qry = index_data.get(t.redis_prefix, None)
        if not qry: pass
        print(f"{hr} INDEXING DOCS: {t.redis_prefix}")
        rdb.redis_client.execute_command(qry)
        print(f"DONE {hr}")


def start_migration():
    tables = get_tables()
    migrate_db(tables)
    index_redis_docs(tables)
        


if __name__ == "__main__":
    print(f"{hr}MIGRATION STARTED{hr}")
    start_migration()
    print(f"{hr}MIGRATION COMPLETED{hr}")
