# 10 to 17
user_input = "SHARED_PERSON_IDNTFCATN"

print ("Passed Argument : " + user_input)

if (not user_input):
    print("Table name not specified.")
    exit

    #21 to 23
INGEST_DB = "DBI:mysql:ingest_db:localhost;root;ClientDemo1023"
INTEGRAL_DB = "DBI:mysql:integral_db:localhost;root;ClientDemo1023"
ACCESS_DB = "DBI:mysql:access_db:localhost;root;ClientDemo1023"

#26 to 31
src_db = "integral_db"
tgt_db = "access_db"

tgt_tablename = user_input#"SHARED_PERSON_IDNTFCATN"
src_tablename = tgt_tablename
src_tablename = src_tablename.replace("SHARED_", "")

print("Loading of data from " + src_tablename + " -> " + tgt_tablename)

#35 tp 47

conn_param = ACCESS_DB.split(';')
conn = MySQLdb.connect(conn_param[0], conn_param[1], conn_param[2], conn_param[3])
get_etl_flow_id = "SELECT etl_flow_id from etl_mdt_db.etl_flow_sql where etl_table = '%s'" % tgt_tablename
cursor = conn.cursor()
cursor.execute(get_etl_flow_id)
flow_id = cursor.fetchone()
print ("\nFlow id for loading table %s is : %s\n" % (tgt_tablename, flow_id[0]))

#49 till 66

#Create the table mapping
tgt_columns = []
src_columns = []
get_columns_query = "select tgt.column_name,src.column_name
from information_schema.columns tgt left outer join information_schema.columns src on tgt.column_name=src.column_name
where tgt.table_name = '%s' and src.table_name='%s' and src.table_schema = '%s' and tgt.table_schema = '%s';" % (tgt_tablename,src_tablename,src_db,tgt_db)
sth = dbh.prepare(get_columns_query)
sth.execute() or raise Exception("Faled to execute query for mapping source with target")
row = sth.fetchrow_array()
while row:
    tgt_column = row[0]
    src_column = row[1]
    if tgt_column == "etl_flow_id":
        src_column = flow_Id
    tgt_columns.append(tgt_column)
    src_columns.append(src_column)
    #print(src_column."->".$tgt_column."\n");
    row = sth.fetchrow_array()

# 68 till 77

""" 
Create the Insert query
"""
column_string = ",".join(tgt_columns)
value_string = ",".join(src_columns)
frame_insert_query = "INSERT INTO %s.%s (%s) SELECT %s FROM %s.%s;" % (tgt_db,tgt_tablename,column_string,value_string,src_db,src_tablename)
print "frame_insert_query = " + frame_insert_query

""" 
Connect to the database
"""
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (tgt_server,tgt_db,tgt_user,tgt_pass))
cursor = cnxn.cursor()

""" 
Execute the query
"""
cursor.execute(frame_insert_query)
cursor.commit()
cursor.close()
cnxn.close()