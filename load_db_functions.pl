#/usr/local/bin/perl
use warnings;
use DBI;
use Data::Dumper qw(Dumper);

sub read_table_name(){
    #Table to load is passed as argument
    #my @args_passed= @ARGV;
    #my $user_input = $args_passed[0];
    my $user_input = "SHARED_PERSON_IDNTFCATN";
    print ("Passed Argument : $user_input"."\n");
    if (not defined($user_input)){
        print("Table name not specified.");
        exit;
    }
    return  $user_input;
}

# MySQL database configuration
sub get_conn_param(){
    my $conn_file_name = "conn_param.txt";
    open(my $fh, '<', $conn_file_name);
    my $line = <$fh>;
    my @conn_param = split(';',$line);
    return @conn_param;
}

sub get_src_table_name(){
    my $src_tn = $tgt_tablename;
    $src_tn =~s/SHARED_//;
    print("Loading of data from $src_tn->$tgt_tablename");
    return $src_tn;
}

#Get This flow's id from metadata table
sub get_flow_id(){
    my $get_etl_flow_id = "SELECT etl_flow_id from etl_mdt_db.etl_flow_sql where etl_table = '$tgt_tablename'";
    $sth = $dbh->prepare($get_etl_flow_id);
    $sth->execute() or die "Faled to execute query to fetch flow id";
    my $flow_Id = $sth->fetchrow();
    print ("\nFlow id for loading table $tgt_tablename is : $flow_Id\n");
    return $flow_Id;
}

#Create the table mapping
sub create_mapping(){
    my $get_columns_query = "select tgt.column_name,src.column_name
from information_schema.columns tgt left outer join information_schema.columns src on tgt.column_name=src.column_name
and src.table_name='$src_tablename' and src.table_schema = '$src_db'
where tgt.table_schema = '$tgt_db' and tgt.table_name = '$tgt_tablename'";
    $sth = $dbh->prepare($get_columns_query);
    $sth->execute() or die "Faled to execute query for mapping source with target";
    while(my @row = $sth->fetchrow_array()){
        my $tgt_column = $row[0];
        my $src_column = $row[1];
        if ($tgt_column eq "etl_flow_id"){
            $src_column = $flow_Id;
        }
        if ($tgt_column eq "etl_transactn_date"){
            $src_column = 'curdate()';
        }
        push(@tgt_columns,$tgt_column);
        push(@src_columns,$src_column);
    }
    my $col_string = join(",",@tgt_columns);
    my $val_string = join(",",@src_columns);
    return $col_string,$val_string;
}

$tgt_tablename = read_table_name();
$src_db = "integral_db";
$tgt_db = "access_db";
$src_tablename = get_src_table_name();

my @conn_param = get_conn_param();
$dbh = DBI->connect($conn_param[0],$conn_param[1],$conn_param[2]);

$flow_Id = get_flow_id();
($column_string,$value_string) = create_mapping();
print ("\nColumn String : $column_string\n");
#Create the Insert query
my $frame_insert_query = "INSERT INTO $tgt_db.$tgt_tablename ($column_string)
SELECT $value_string FROM $src_db.$src_tablename;";
print ("\nQuery for loading data to $tgt_db.$tgt_tablename is : $frame_insert_query\n");
$sth = $dbh->prepare($frame_insert_query);
$sth->execute() or die "Faled to execute Insert query";
$sth->finish();

$dbh->disconnect();