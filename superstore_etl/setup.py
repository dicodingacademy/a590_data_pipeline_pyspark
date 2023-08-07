import os

PWD = os.environ['SUPPERSTORE_PASS']
USER_ID = "postgres"
SERVER = "db.oqucojswwhlmdptxhcaa.supabase.co"
DB = "postgres"
DRIVER = "org.postgresql.Driver"

# source connection
src_url = f"jdbc:postgresql://{SERVER}:5432/{DB}?user={USER_ID}&password={PWD}"

# target connection
target_url = f"jdbc:postgresql://{SERVER}:5432/{DB}?user={USER_ID }&password={PWD}"