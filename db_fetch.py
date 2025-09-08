import os
import mysql.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Database connect
db = mysql.connector.connect(
    host = os.getenv('dsk_mysql_host'),
    user = os.getenv('dsk_mysql_user'),
    password = os.getenv('dsk_mysql_pwd'),
    database = os.getenv('dsk_mysql_database')
) 

cursor = db.cursor()
query = "SELECT * FROM carbon_footprint"
cursor.execute(query)
rows = cursor.fetchall()
column_names = [i[0] for i in cursor.description]
dsk_table = pd.DataFrame(rows, columns=column_names)

cursor.close()
db.close()
