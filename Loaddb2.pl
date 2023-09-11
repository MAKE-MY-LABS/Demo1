#/usr/local/bin/perl
use strict;
use warnings;
use DBI;
use Data::Dumper qw(Dumper);

#Table to load is passed as argument
#my @args_passed= @ARGV;
#my $user_input = $args_passed[0];
my $user_input = "SHARED_PERSON_IDNTFCATN";

print ("Passed Argument : $user_input"."\n");

if (not defined($user_input)){
    print("Table name not specified.");
    exit;
}

# MySQL database configuration
#Move below to config file and use that instead of hardcoded values
my $INGEST_DB = "DBI:mysql:ingest_db:localhost;root;ClientDemo1023";
my $INTEGRAL_DB = "DBI:mysql:integral_db:localhost;root;ClientDemo1023";
my $ACCESS_DB = "DBI:mysql:access_db:localhost;root;ClientDemo1023";

#Source database
my $src_db = "integral_db";
my $tgt_db = "access_db";

my $tgt_tablename = $user_input;#"SHARED_PERSON_IDNTFCATN";
my $src_tablename = $tgt_tablename;
$src_tablename =~s/SHARED_//;

print("Loading of data from $src_tablename->$tgt_tablename");

#Connect to Database
my @conn_param = split($ACCESS_DB,';');
#my $dbh  = DBI->connect("$conn_param[0]","$conn_param[1]","$conn_param[2]") or die "Cannot connect to Mysql database";
my $dbh = DBI->connect('DBI:mysql:integral_db:localhost','root','ClientDemo1023');

my $sth;

#Get This flow's id from metadata table
my $get_etl_flow_id = "SELECT etl_flow_id from etl_mdt_db.etl_flow_sql where etl_table = '$tgt_tablename'";
$sth = $dbh->prepare($get_etl_flow_id);
$sth->execute() or die "Faled to execute query to fetch flow id";
my $flow_Id = $sth->fetchrow();
print ("\nFlow id for loading table $tgt_tablename is : $flow_Id\n");

#Create the table mapping
my @tgt_columns;
my @src_columns;
my $get_columns_query = "select tgt.column_name,src.column_name
from information_schema.columns tgt left outer join information_schema.columns src on tgt.column_name=src.column_name
where tgt.table_name = '$tgt_tablename' and src.table_name='$src_tablename' and src.table_schema = '$src_db' and tgt.table_schema = '$tgt_db';";
$sth = $dbh->prepare($get_columns_query);
$sth->execute() or die "Faled to execute query for mapping source with target";
while(my @row = $sth->fetchrow_array()){
    my $tgt_column = $row[0];
    my $src_column = $row[1];
    if ($tgt_column eq "etl_flow_id"){
        $src_column = $flow_Id;
    }
    push(@tgt_columns,$tgt_column);
    push(@src_columns,$src_column);
    #print($src_column."->".$tgt_column."\n");
}

#Create the Insert query
my $column_string = join(",",@tgt_columns);
my $value_string = join(",",@src_columns);
my $frame_insert_query = "INSERT INTO $tgt_db.$tgt_tablename ($column_string)
SELECT $value_string FROM $src_db.$src_tablename;";
$sth = $dbh->prepare($frame_insert_query);
$sth->execute() or die "Faled to execute Insert query";

$sth->finish();
$dbh->disconnect();