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





