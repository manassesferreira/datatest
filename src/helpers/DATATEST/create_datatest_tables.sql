DROP TABLE IF EXISTS DIM_TIME;

CREATE TABLE DIM_TIME
(
TIME_ID bigint COMMENT "Seconds since linux epoch"
,TIME_DATE date COMMENT "yyyy-MM-dd"
,TIME_OFDAY string COMMENT "HH:mm:ss"
,TIME_YEAR int
,TIME_MONTH int
,TIME_DAY int
,TIME_HOUR int
,TIME_MINUTE int
,TIME_SECOND int
)
COMMENT 'Date Dimension Table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DROP TABLE IF EXISTS DIM_TYPE;

CREATE TABLE DIM_TYPE
(
TYPE_ID bigint
,NAME string
,DESC string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DROP TABLE IF EXISTS DIM_ENTITY;

CREATE TABLE DIM_ENTITY
(
ENTITY_ID bigint
,ENTITY_IS_PRE_SQL boolean COMMENT 'Entity boolean for is preSQL'
,ENTITY_ROW string COMMENT 'Entity string for row value'
,ENTITY_COLUMN string COMMENT 'Entity string for column name'
,ENTITY_TABLE string COMMENT 'Entity string for table name'
,ENTITY_SCHEMA string COMMENT 'Entity string for schema name'
,ENTITY_AIRFLOW_CONNECTION string COMMENT 'Entity string for airflow connection uuid name'
,ENTITY_PRE_SQL_PATH string 
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DROP TABLE IF EXISTS FACT_TEST;

CREATE TABLE FACT_TEST
(
TEST_TYPE_ID bigint
,TEST_1_ENTITY_ID bigint COMMENT 'Target entity id'
,TEST_1_VARIABLE double COMMENT 'Target entity variable'
,TEST_1_MEASURE bigint COMMENT 'Target entity measure'
,TEST_2_ENTITY_ID bigint COMMENT 'Source entity id'
,TEST_2_VARIABLE double COMMENT 'Source entity variable'
,TEST_2_MEASURE bigint COMMENT 'Source entity measure'
,TEST_ASSERT boolean
,TEST_ASSERT_TIME_ID bigint
,TEST_ASSERT_DATE date
)
COMMENT 'Data test table, fact table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/data/DATATEST.DIM_TIME.csv' OVERWRITE INTO TABLE DIM_TIME;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/data/DATATEST.DIM_TYPE.csv' OVERWRITE INTO TABLE DIM_TYPE;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/data/DATATEST.DIM_ENTITY.csv' OVERWRITE INTO TABLE DIM_ENTITY;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/data/DATATEST.FACT_TEST.csv' OVERWRITE INTO TABLE FACT_TEST;