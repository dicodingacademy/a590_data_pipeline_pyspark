import os

PWD = os.environ['PGPASS']
USER_ID = "postgres"
SERVER = "db.oqucojswwhlmdptxhcaa.supabase.co"
DB = "postgres"
DRIVER = "org.postgresql.Driver"

SERVER_TARGET = "db.rstxdaajwmqpconknzkv.supabase.co"

# source connection
src_url = f"jdbc:postgresql://{SERVER}:5432/{DB}?user={USER_ID}&password={PWD}"

# target connection
target_url = f"jdbc:postgresql://{SERVER_TARGET}:5432/{DB}?user={USER_ID }&password={PWD}"