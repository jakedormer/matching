import psycopg2

conn = psycopg2.connect(host="35.228.239.225",
                        port="5432",
                        user="postgres",
                        password="Emoclew30@",
                        database="postgres" # To remove slash
)
cursor = conn.cursor()


# cursor.execute("INSERT INTO a_table (c1, c2, c3) VALUES(%s, %s, %s)", (v1, v2, v3))
cursor.close()
conn.close()

# To connect
# psql -h <host> -p <port> -u <database>

# Commands
# \dt - List all tables
