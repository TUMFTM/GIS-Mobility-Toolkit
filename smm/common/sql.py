import os
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


class DBBase(object):

    def __init__(self):
        super().__init__()
        self.conn = None

    def getSqlAlchemyCon(self):
        return create_engine(self.con_url)

    def switchSchemaTarget(self, target):
        return \
        f"""CREATE SCHEMA IF NOT EXISTS "{target}";
           SET SCHEMA '{target}'; 
           SET search_path = "{target}", public;
        """

    def setConn(self, conn):
        self.conn = conn
        self.con_url = str(
            URL(drivername="postgresql",
                host=conn.info.host,
                port=conn.info.port,
                username=conn.info.user,
                password=conn.info.password,
                database=conn.info.dbname))
        return self

    def setupDB(self, host, port, username, password, database, extensions=None):
        self.host = f"{host}:{port}"
        self.username = username
        self.password = password
        self.database = database
        extensions = extensions or []

        #Fixes upload errors
        keepalive_kwargs = {
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 5,
            "keepalives_count": 5,
        }

        #Check extensions
        self.conn = psycopg2.connect(dbname=database,
                                     user=username,
                                     password=password,
                                     host=host,
                                     port=port,
                                     **keepalive_kwargs)
        self.con_url = str(
            URL.create(drivername="postgresql",
                       host=host,
                       port=port,
                       username=username,
                       password=password,
                       database=database).render_as_string(hide_password=False))
        sql = ["SET SCHEMA 'public';"]
        for e in extensions:
            sql.append(f"CREATE EXTENSION IF NOT EXISTS {e};")
        sql = "\n".join(sql)
        with self.conn.cursor() as curs:
            curs.execute(sql)
        self.conn.commit()
        return self

    def executeSQL(self, sql, placeholders={}):
        for key, value in placeholders.items():
            sql = sql.replace(f"${key}", value)
        with self.conn.cursor() as curs:
            curs.execute(sql)
        self.conn.commit()
        return self


class SQLTemplateManager:

    def __init__(self, template_dir):
        self.template_dir = template_dir

    def load(self, name, replacements=None):
        if not name.endswith(".sql"):
            name += ".sql"
        with open(os.path.join(self.template_dir, name)) as f:
            sql_template = f.read()

        # Substitute placeholders
        #sql_template = sql_template.replace("{}", "")    # Replace all {}-sequences without specifiers
        if replacements is not None:
            sql = sql_template
            for k, v in replacements.items():
                sql = sql.replace("$" + k, v)
        else:
            sql = sql_template

        return sql
