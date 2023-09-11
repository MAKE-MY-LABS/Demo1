//File file = new File("C:/Users/906371/Demo/INTG_BNK_CRDT_PERSON_IDNTFCATN_V2.xml")
import groovy.xml.XmlSlurper
import groovy.sql.Sql
File file = new File("C:/Users/906371/Demo/INTG_BNK_CRDT_PERSON_IDNTFCATN_V2.xml")
String fileContent = file.text
def parsed = new XmlSlurper().parseText(fileContent)
def map = [:]
def sql_query = new StringBuffer();
def column_code = parsed.column_mappings.column_mapping.col_code*.text()
String column_codeIds = column_code.join(",") 
String select_query = "Select"
for(node in parsed.column_mappings.column_mapping)
{
    select_query += " ${node.rule.text()} AS ${node.col_code.text()},"
}
sql_query = "INSERT INTO integral_db.${parsed.physical_table_name.text()} (${column_codeIds}) ${select_query.substring(0, select_query.length()-1)} From (${parsed.src_query.text()})ingst;"
def sql = Sql.newInstance("jdbc:mysql://localhost:3306/etl_mdt_db","root","Access@2023","com.mysql.cj.jdbc.Driver")
def row = sql.firstRow("""SELECT MAX(etl_flow_id) as max FROM etl_mdt_db.etl_flow_sql;""")
def maxId = row.max
if(maxId==null)
{
    maxId = 1001
}
else
{
    maxId = maxId + 1
}
sql.executeInsert("""INSERT INTO etl_mdt_db.etl_flow_sql(etl_flow_id, etl_flow_sql, etl_db_layr) 
VALUES ($maxId, '$sql_query', 'integral_db');""".toString())
sql.close()