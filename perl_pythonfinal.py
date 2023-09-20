user_input = "SHARED_PERSON_IDNTFCATN"

print("Passed Argument : " + user_input + "\n")

if (user_input is not None):
    print("Table name not specified.")
    exit()

# MySQL database configuration
#Move below to config file and use that instead of hardcoded values
INGEST_DB = "DBI:mysql:ingest_db:localhost;root;ClientDemo1023"
INTEGRAL_DB = "DBI:mysql:integral_db:localhost;root;ClientDemo1023"
ACCESS_DB = "DBI:mysql:access_db:localhost;root;ClientDemo1023"

#Source database
src_db = "integral_db"
tgt_db = "access_db"
tgt_tablename = user_input
src_tablename = tgt_tablename.replace("SHARED_","")
print("Loading of data from $src_tablename->$tgt_tablename");

#Connect to Database
conn_param = ACCESS_DB.split(';')
dbh = MySQLdb.connect(host=conn_param[0],user=conn_param[1],passwd=conn_param[2],db=conn_param[3])
cursor = dbh.cursor()

sth = None

#Get This flow's id from metadata table
get_etl_flow_id = "SELECT etl_flow_id from etl_mdt_db.etl_flow_sql where etl_table = '" + tgt_tablename + "'"
cursor = conn.cursor()
cursor.execute(get_etl_flow_id)
flow_Id = cursor.fetchone()
print ("\nFlow id for loading table " + tgt_tablename + " is : " + str(flow_Id) + "\n")

#Create the table mapping
tgt_columns = []
src_columns = []
get_columns_query = "select tgt.column_name,src.column_name from information_schema.columns tgt left outer join information_schema.columns src on tgt.column_name=src.column_name and src.table_name='" + src_tablename + "' and src.table_schema = '" + src_db + "' where tgt.table_schema = '" + tgt_db + "' and tgt.table_name = '" + tgt_tablename + "'"
sth = dbh.prepare(get_columns_query)
sth.execute() or die("Faled to execute query for mapping source with target")
while True:
    row = sth.fetchone()
    if row is None:
        break
    tgt_column = row[0]
    src_column = row[1]
    if tgt_column == "etl_flow_id":
        src_column = flow_Id
    if tgt_column == "etl_transactn_date":
        src_column = 'curdate()'
    tgt_columns.append(tgt_column)
    src_columns.append(src_column)

#Create the Insert query
column_string = ','.join(tgt_columns)
value_string = ','.join(src_columns)
frame_insert_query = "INSERT INTO %s.%s (%s) SELECT %s FROM %s.%s;" % (tgt_db, tgt_tablename, column_string, value_string, src_db, src_tablename)
print ("\nQuery for loading data to %s.%s is : %s\n" % (tgt_db, tgt_tablename, frame_insert_query))
cursor = connection.cursor()
cursor.execute(frame_insert_query)
connection.commit()
cursor.close()






