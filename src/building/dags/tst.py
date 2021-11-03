from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from pprint import pprint
from kafka import KafkaProducer
from json import dumps
import time, json, datetime
from random import randrange

from select import select
from airflow.exceptions import AirflowException
from airflow.configuration import conf
from base64 import b64encode

class dType:
    def __init__(self, name=''):
        self._name = name

    def getName(self):
        return self._name

    def setName(self, name):
        self._name=name

class dEntity:
    def __init__(self, airflow_connection='', schema='', table='', column='', row=[], isPreSQL='', pk=''):
        self._airflow_connection =  airflow_connection
        self._schema = schema
        self._table = table
        self._column = column
        self._row = row
        self._isPreSQL = isPreSQL
        self._pk = pk

    def getTable(self):
        return self._table

class fTest:

    def __init__(self):
        self._type = dType()
        self._first_entity = dEntity()
        self._second_entity = dEntity()

    def getType(self):
        return self._type

    def setType(self, dType):
        self._type = dType

    def getFirstEntity(self):
        return self._first_entity

    def setFirstEntity(self, airflow_connection, schema, table, column, row, isPreSQL, pk):
        entity = dEntity( airflow_connection, schema, table, column, row, isPreSQL, pk )
        self._first_entity = entity

    def getSecondEntity(self):
        return self._second_entity

    def setSecondEntity(self, airflow_connection, schema, table, column, row, isPreSQL, pk):
        entity = dEntity( airflow_connection, schema, table, column, row, isPreSQL, pk )
        self._second_entity = entity

    def get_test(**kwargs):
        try:
            #__class__.get_test_manual(**kwargs)
            __class__.get_test_json(**kwargs)
            #__class__.get_test_db(**kwargs)
        except:
            raise AirflowException(f"error getting test")

    def run_sql_get_dataframe(hook, sql):
        print(sql)
        df = hook.get_pandas_df(sql=sql)
        print(df)
        return df


    def get_test_db(**kwargs):
        """Get test from fact_test when the dag is run manually with config like this: """
        """
{
  "test_source":"db"
}
        """
        hook = JdbcHook(jdbc_conn_id='kylin')

        sql= "select test_1_entity_id as TARGET, count(*) as QTD from datatest.fact_test group by test_1_entity_id order by QTD limit 1"
        pk_1 = str(__class__.run_sql_get_dataframe(hook, sql)['TARGET'].iloc[0])
        sql= "select ENTITY_ROW, ENTITY_COLUMN, ENTITY_TABLE, ENTITY_SCHEMA, ENTITY_AIRFLOW_CONNECTION from datatest.dim_entity where entity_id = "+pk_1
        entity_1 =  __class__.run_sql_get_dataframe(hook, sql)
        kwargs['ti'].xcom_push( key='dst_conn', value= entity_1['ENTITY_AIRFLOW_CONNECTION'].iloc[0] )
        kwargs['ti'].xcom_push( key='dst_ent', value=  entity_1['ENTITY_SCHEMA'].iloc[0] +"."+  entity_1['ENTITY_TABLE'].iloc[0] )
        kwargs['ti'].xcom_push( key='dst_col', value= entity_1['ENTITY_COLUMN'].iloc[0] )
        kwargs['ti'].xcom_push( key='dst_row', value= entity_1['ENTITY_ROW'].iloc[0].split("|") )
        kwargs['ti'].xcom_push( key='dst_pk', value= pk_1 )

        sql = "select test_2_entity_id as SOURCE, count(*) as QTD from datatest.fact_test where test_1_entity_id = "+pk_1+" group by test_2_entity_id order by QTD limit 1"
        pk_2 = str(__class__.run_sql_get_dataframe(hook, sql)['SOURCE'].iloc[0])
        sql= "select ENTITY_ROW, ENTITY_COLUMN, ENTITY_TABLE, ENTITY_SCHEMA, ENTITY_AIRFLOW_CONNECTION from datatest.dim_entity where entity_id = "+pk_2
        entity_2 =  __class__.run_sql_get_dataframe(hook, sql)
        kwargs['ti'].xcom_push( key='org_conn', value= entity_2['ENTITY_AIRFLOW_CONNECTION'].iloc[0] )
        kwargs['ti'].xcom_push( key='org_ent', value=  entity_2['ENTITY_SCHEMA'].iloc[0] +"."+  entity_2['ENTITY_TABLE'].iloc[0] )
        kwargs['ti'].xcom_push( key='org_col', value= entity_2['ENTITY_COLUMN'].iloc[0] )
        kwargs['ti'].xcom_push( key='org_row', value= entity_2['ENTITY_ROW'].iloc[0].split("|") )
        kwargs['ti'].xcom_push( key='org_pk', value= pk_2 )

    def get_test_manual(**kwargs):
        """Get test from input in web interface when the dag is run manually with config like this:"""
        """
{
  "1_airflow_connection":"oracle",
  "1_schema":"armazemdwdba",
  "1_table":"acd_fato_matricula",
  "1_column":"sk_semestre",
  "1_row":["20201", "20202", "20211"],
  "1_isPreSQL":false,
  "2_airflow_connection":"oracle",
  "2_schema":"academico25",
  "2_table":"matricula",
  "2_column":"plt_codigo_chr",
  "2_row":["20201", "20202", "20211"],
  "2_isPreSQL":false
}
        """

        kwargs['ti'].xcom_push( key='dst_conn', value= kwargs['dag_run'].conf['1_airflow_connection'] )
        kwargs['ti'].xcom_push( key='dst_ent', value= kwargs['dag_run'].conf['1_schema'] +"."+ kwargs['dag_run'].conf['1_table'] )
        kwargs['ti'].xcom_push( key='dst_col', value= kwargs['dag_run'].conf['1_column'])
        kwargs['ti'].xcom_push( key='dst_row', value= kwargs['dag_run'].conf['1_row'] )
        kwargs['ti'].xcom_push( key='dst_pk', value="42" )  #todo: get pk from base if not exists then create entity

        kwargs['ti'].xcom_push( key='org_conn', value= kwargs['dag_run'].conf['2_airflow_connection'] )
        kwargs['ti'].xcom_push( key='org_ent', value= kwargs['dag_run'].conf['2_schema'] +"."+ kwargs['dag_run'].conf['2_table'] )
        kwargs['ti'].xcom_push( key='org_col', value= kwargs['dag_run'].conf['2_column'])
        kwargs['ti'].xcom_push( key='org_row', value= kwargs['dag_run'].conf['2_row'] )
        kwargs['ti'].xcom_push( key='org_pk', value="53" )  #todo: get pk from base if not exists then create entity


    def get_test_json(**kwargs):
        """Get test from json file"""

        with open("/opt/airflow/dags/fact_test.json", "r") as fp:
            fTests = json.load(fp)

        with open("/opt/airflow/dags/dim_entity.json", "r") as fp:
            dEntitys = json.load(fp)

        fTests_new={}
        test_founded=False
        for id in fTests:
            fTest_new = fTests[id]
            fTest = fTests[id]
            if fTest['TEST_ASSERT_TIME_ID'] == '' and not test_founded:
#               if fTest['type'] == 'comparacao':
                fTest_new['TEST_ASSERT_TIME_ID']=int(time.time())
                test_founded=True
                pk1=fTest['TEST_1_ENTITY_ID']
                pk2=fTest['TEST_2_ENTITY_ID']
                for ENTITY_ID in dEntitys:
                    dEntity = dEntitys[ENTITY_ID]
                    pk=dEntity['ENTITY_ID']
                    if pk1 == pk:
                        kwargs['ti'].xcom_push( key='dst_conn', value=dEntity['ENTITY_AIRFLOW_CONNECTION'] )
                        kwargs['ti'].xcom_push( key='dst_pk', value=pk1 )
                        kwargs['ti'].xcom_push( key='dst_ent', value=dEntity['ENTITY_SCHEMA']+"."+dEntity['ENTITY_TABLE'] )
                        kwargs['ti'].xcom_push( key='dst_col', value=dEntity['ENTITY_COLUMN'] )
                        kwargs['ti'].xcom_push( key='dst_row', value=dEntity['ENTITY_ROW'] )
                        kwargs['ti'].xcom_push( key='dst_filter_key', value=dEntity['ENTITY_FILTER_KEY'] )
                        kwargs['ti'].xcom_push( key='dst_filter_value', value=dEntity['ENTITY_FILTER_VALUE'] )
                        kwargs['ti'].xcom_push( key='dst_is_pre_sql', value=dEntity['ENTITY_IS_PRE_SQL'] )
                        kwargs['ti'].xcom_push( key='dst_pre_sql_path', value=dEntity['ENTITY_PRE_SQL_PATH'] )
                    if pk2 == pk:
                        kwargs['ti'].xcom_push( key='org_conn', value=dEntity['ENTITY_AIRFLOW_CONNECTION'] )
                        kwargs['ti'].xcom_push( key='org_pk', value=pk2 )
                        kwargs['ti'].xcom_push( key='org_ent', value=dEntity['ENTITY_SCHEMA']+"."+dEntity['ENTITY_TABLE'] )
                        kwargs['ti'].xcom_push( key='org_col', value=dEntity['ENTITY_COLUMN'] )
                        kwargs['ti'].xcom_push( key='org_row', value=dEntity['ENTITY_ROW'] )
                        kwargs['ti'].xcom_push( key='org_filter_key', value=dEntity['ENTITY_FILTER_KEY'] )
                        kwargs['ti'].xcom_push( key='org_filter_value', value=dEntity['ENTITY_FILTER_VALUE'] )
                        kwargs['ti'].xcom_push( key='org_is_pre_sql', value=dEntity['ENTITY_IS_PRE_SQL'] )
                        kwargs['ti'].xcom_push( key='org_pre_sql_path', value=dEntity['ENTITY_PRE_SQL_PATH'] )
            fTests_new[id] = fTest_new

        if test_founded:
            with open("/opt/airflow/dags/fact_test.json", "w") as fp:
                json.dump(fTests_new, fp)

        assert test_founded


    def save_test_with_ssh_hive(sql):
        """Save test using ssh and hive command"""

        home='/home/admin/'
        kylin="apache-kylin-4.0.0-bin-spark2"
        home_kylin=home+kylin

        paths=[]
        paths.append("/usr/local/sbin")
        paths.append("/usr/local/bin")
        paths.append("/usr/sbin")
        paths.append("/usr/bin")
        paths.append("/sbin")
        paths.append("/bin")
        paths.append(home+"jdk1.8.0_141/bin")
        paths.append(home+"hadoop-2.8.5/bin")
        paths.append(home+"apache-hive-1.2.1-bin/bin")
        paths.append(home+"hbase-1.1.2/bin")
        paths.append(home+"apache-maven-3.6.1/bin")
        paths.append(home+"spark-2.4.7-bin-hadoop2.7/bin")
        paths.append(home+"kafka_2.11-1.1.1/bin")
        path = "PATH=$PATH:"+':'.join(paths)
#
# apache-hive-1.2.1-bin
# apache-maven-3.6.1
# entrypoint.sh
# hadoop-2.8.5
# kafka_2.11-1.1.1
# spark-2.4.7-bin-hadoop2.7
# zookeeper.out
# apache-kylin-4.0.0-bin-spark2
# create.sh
# first_run
# jdk1.8.0_141  mysql80-community-release-el7-3.noarch.rpm  zookeeper-3.4.6

        exports=[]
        exports.append("JAVA_HOME="+home+"jdk1.8.0_141")
        exports.append("HIVE_VERSION=1.2.1")
        exports.append("HADOOP_VERSION=2.8.5")
        exports.append("HBASE_VERSION=1.1.2")
        exports.append("SPARK_VERSION=2.4.7")
        exports.append("KAFKA_VERSION=1.1.1")
        exports.append("LIVY_VERSION=0.6.0")
        exports.append("MVN_HOME="+home+"apache-maven-3.6.1")
        exports.append("HADOOP_HOME="+home+"hadoop-$HADOOP_VERSION")
        exports.append("HIVE_HOME="+home+"apache-hive-$HIVE_VERSION-bin")
        exports.append("HADOOP_CONF=$HADOOP_HOME/etc/hadoop")
        exports.append("HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop")
        exports.append("HBASE_HOME="+home+"hbase-$HBASE_VERSION")
        exports.append("SPARK_HOME="+home+"spark-$SPARK_VERSION-bin-hadoop2.7")
        exports.append("SPARK_CONF_DIR="+home+"spark-$SPARK_VERSION-bin-hadoop2.7/conf")
        exports.append("KAFKA_HOME="+home+"kafka_2.11-$KAFKA_VERSION")
        exports.append("LIVY_HOME="+home+"apache-livy-$LIVY_VERSION-incubating-bin")
        exports.append("KYLIN_VERSION=4.0.0")
        exports.append("KYLIN_HOME="+home_kylin)
        export = 'export ' + ' && export '.join(exports)

        source = 'source '+home_kylin+'/bin/header.sh && source '+home_kylin+'/bin/load-hive-conf.sh'

        hive = 'hive ${hive_conf_properties} --database DATATEST -e'

        command = path + ' && ' + export + ' && ' + source + ' && ' + hive + ' "' + sql + '"'
        print(sql)
        #print(command)
        timeout=60.0

        ssh_hook = SSHHook(ssh_conn_id='kylin_ssh')
        with ssh_hook.get_conn() as ssh_client:

                # set timeout taken as params
                stdin, stdout, stderr = ssh_client.exec_command(
                    command=command,
                    #get_pty=self.get_pty,
                    timeout=timeout,
                    #environment=self.environment,
                )
                 # get channels
                channel = stdout.channel

                # closing stdin
                stdin.close()
                channel.shutdown_write()

                agg_stdout = b''
                agg_stderr = b''

                # capture any initial output in case channel is closed already
                stdout_buffer_length = len(stdout.channel.in_buffer)

                if stdout_buffer_length > 0:
                    agg_stdout += stdout.channel.recv(stdout_buffer_length)

                # read from both stdout and stderr
                while not channel.closed or channel.recv_ready() or channel.recv_stderr_ready():
                    readq, _, _ = select([channel], [], [], timeout)
                    for recv in readq:
                        if recv.recv_ready():
                            line = stdout.channel.recv(len(recv.in_buffer))
                            agg_stdout += line
                            print(line.decode('utf-8', 'replace').strip('\n'))
                        if recv.recv_stderr_ready():
                            line = stderr.channel.recv_stderr(len(recv.in_stderr_buffer))
                            agg_stderr += line
                            print(line.decode('utf-8', 'replace').strip('\n'))
                    if (
                        stdout.channel.exit_status_ready()
                        and not stderr.channel.recv_stderr_ready()
                        and not stdout.channel.recv_ready()
                    ):
                        stdout.channel.shutdown_read()
                        stdout.channel.close()
                        break

                stdout.close()
                stderr.close()

                exit_status = stdout.channel.recv_exit_status()
                if exit_status == 0:
                    enable_pickling = conf.getboolean('core', 'enable_xcom_pickling')
                    if enable_pickling:
                        return agg_stdout
                    else:
                        return b64encode(agg_stdout).decode('utf-8')

                else:
                    error_msg = agg_stderr.decode('utf-8')
                    raise AirflowException(f"error running cmd: {command}, error: {error_msg}")

class Test(fTest):
    def __init__(self):
        super().setType( dType('comparacao') )

    def getTypeName(self):
        return super().getType().getName()

    def getFirstEntityTable(self):
        return super().getFirstEntity().getTable()

    def getSecondEntityTable(self):
        return super().getSecondEntity().getTable()

    def collect(fonte, **kwargs):
        """Collect from fonte"""

        ti = kwargs['ti']
        if fonte == 'origem':
            pk = ti.xcom_pull(key='org_pk', task_ids='get_test')
            conn = ti.xcom_pull(key='org_conn', task_ids='get_test')
            ent = ti.xcom_pull(key='org_ent', task_ids='get_test')
            col = ti.xcom_pull(key='org_col', task_ids='get_test')
            row = ti.xcom_pull(key='org_row', task_ids='get_test')
            filter_key = ti.xcom_pull(key='org_filter_key', task_ids='get_test')
            filter_val = ti.xcom_pull(key='org_filter_value', task_ids='get_test')

            is_pre_sql =ti.xcom_pull(key='org_is_pre_sql', task_ids='get_test')
            if is_pre_sql:
                pre_sql_path = ti.xcom_pull(key='org_pre_sql_path', task_ids='get_test')
        elif fonte == 'destino':
            pk = ti.xcom_pull(key='dst_pk', task_ids='get_test')
            conn = ti.xcom_pull(key='dst_conn', task_ids='get_test')
            ent = ti.xcom_pull(key='dst_ent', task_ids='get_test')
            col = ti.xcom_pull(key='dst_col', task_ids='get_test')
            row = ti.xcom_pull(key='dst_row', task_ids='get_test')
            filter_key = ti.xcom_pull(key='dst_filter_key', task_ids='get_test')
            filter_val = ti.xcom_pull(key='dst_filter_value', task_ids='get_test')

            #para mim hoje nao faz sentido fazer presql no destino mas paciencia vai que surge algo
            is_pre_sql =ti.xcom_pull(key='dst_is_pre_sql', task_ids='get_test')
            if is_pre_sql:
                pre_sql_path = ti.xcom_pull(key='dst_pre_sql_path', task_ids='get_test')


        #print(pk)
        #print(conn)
        #print(ent)
        #print(col)
        #print(row)

        sql=""

        if is_pre_sql:
            try:
                 fp=open(pre_sql_path, "r")
                 sql="with PRESQL as (\n"
                 sql=sql+fp.read()
                 sql=sql+")\n"
                 fp.close()
            except:
                raise AirflowException(f"error getting pre sql file")

        if col != '':
            sql=sql+"select "+col+" as COL, count(*) as QTD from "
        else:
            sql=sql+"select count(*) as QTD from "

        if conn in ['oracle', 'academico', 'armazem']:
            hook = OracleHook(oracle_conn_id=conn)

            if is_pre_sql:
                sql=sql+"PRESQL"
            else:
                sql=sql+ent

        elif conn == 'kylin':
            hook = JdbcHook(jdbc_conn_id=conn)

            if is_pre_sql:
                sql=sql+"PRESQL"
            else:
                sql=sql+ent.split(".")[1]



        keys=len(filter_key)
        if keys > 0:
            where_filter=''
            i=0
            for key in filter_key:
                values=len(filter_val[i])
                vs='('
                j=0
                for v in filter_val[i]:
                    vs=vs+"'"+v+"'"
                    if j < values-1:
                        vs=vs+', '
                    j=j+1
                vs=vs+')'
                where_filter=' '+key+' in '+vs
                if i < keys-1:
                    where_filter = where_filter + ' and'
                i=i+1

            where_filter = ' where'+where_filter
            sql = sql + where_filter

        if col != '':
            sql=sql+" group by "+col+ " order by COL desc"
        print(sql)

        df = hook.get_pandas_df(sql=sql)
        print(df)

        qty_founded=0
        if col != '':
            if len(row)>0:
                for index, line in df.iterrows():
                    for r in row:
                        if str(line['COL']).replace('.0','') == r:
                            v=str(line['QTD']).replace('.0','')

                            #print(pk+" "+r+" "+v)
                            kwargs['ti'].xcom_push( key=pk+"."+r, value= v )

                            qty_founded=qty_founded+1
                            break
                    if qty_founded == len(row):
                        break
            else:
                entity_row=[]
                for index, line in df.iterrows():
                    r=str(line['COL'])
                    v=str(line['QTD'])
                    kwargs['ti'].xcom_push( key=pk+"."+r, value= v )
                    entity_row.append(r)

                if fonte == 'destino':
                    kwargs['ti'].xcom_push( key='dst_row', value=entity_row )
                elif fonte == 'origem':
                    kwargs['ti'].xcom_push( key='org_row', value=entity_row )
        else:
            v=df['QTD'].iloc[0]
            kwargs['ti'].xcom_push( key=pk, value= v )




    def _assert_and_save_test(**kwargs):
        """Assert"""

        ti = kwargs['ti']
        dst_pk = ti.xcom_pull(key='dst_pk', task_ids='get_test')
        dst_row = ti.xcom_pull(key='dst_row', task_ids='get_test')
        org_pk = ti.xcom_pull(key='org_pk', task_ids='get_test')
        org_row = ti.xcom_pull(key='org_row', task_ids='get_test')

        dlen=len(dst_row)
        olen=len(org_row)
        if dlen == 0 and olen == 0:
            dst_row = ti.xcom_pull(key='dst_row', task_ids='collect_target')
            org_row = ti.xcom_pull(key='org_row', task_ids='collect_source')


        d = {}
        for r in dst_row:
            d[r]= ti.xcom_pull( key= dst_pk+"."+r, task_ids='collect_target' )
        o = {}
        for r in org_row:
            o[r]= ti.xcom_pull( key= org_pk+"."+r, task_ids='collect_source' )


        if dlen == 0 and olen == 0:
            shared_key_not_null_items = {k: d[k] for k in d if k in o and ( d[k] is not None or o[k] is not None )}
            #print(shared_key_not_null_items)

        dst_keys=[]
        dst_vals=[]
        org_keys=[]
        org_vals=[]
        do_asrts=[]

        #i=0
        if dlen == 0 and olen == 0:
            for k in shared_key_not_null_items:
                #si=str(i)
                vd = d[k]
                vo = o[k]

                a = vd == vo

                #print (k, vd)
                #print (k, vo)
                #print (a)

                dst_keys.append(k)  #kwargs['ti'].xcom_push( key=dst_pk+'>'+si, value=k ) # > variable
                dst_vals.append(vd) #kwargs['ti'].xcom_push( key=dst_pk+'.'+si, value=vd ) # . measure
                org_keys.append(k)  #kwargs['ti'].xcom_push( key=org_pk+'>'+si, value=k )
                org_vals.append(vo) #kwargs['ti'].xcom_push( key=org_pk+'.'+si, value=vo )
                do_asrts.append(a)  #kwargs['ti'].xcom_push( key='a.'+si, value=a )

                #i=i+1


        else:
            for (kd,vd), (ko,vo) in zip(d.items(), o.items()):
                #si=str(i)

                a = vd == vo and vd is not None and vo is not None

                #print (kd, vd)
                #print (ko, vo)
                #print (a)

                dst_keys.append(k)  #kwargs['ti'].xcom_push( key=dst_pk+'>'+si, value=kd ) # > key
                dst_vals.append(vd) #kwargs['ti'].xcom_push( key=dst_pk+'.'+si, value=vd ) # . value
                org_keys.append(k)  #kwargs['ti'].xcom_push( key=org_pk+'>'+si, value=ko )
                org_vals.append(vo) #kwargs['ti'].xcom_push( key=org_pk+'.'+si, value=vo )
                do_asrts.append(a)  #kwargs['ti'].xcom_push( key='a.'+si, value=a )

                #i=i+1

        #kwargs['ti'].xcom_push( key='numberOfAsserts', value=i )


        """Save test"""

        QUERY_LIMIT_DUE_SSH_TOO_LONG=1000
        n=len(do_asrts)

        if all(v is not None for v in [n, dst_pk, org_pk]):
            t=str(int(time.time()))
            s=datetime.date.today().isoformat()

            for beg in range(0,n,QUERY_LIMIT_DUE_SSH_TOO_LONG):
                end=beg+QUERY_LIMIT_DUE_SSH_TOO_LONG
                if end > n:
                    end=n

                values=""
                for i in range(beg,end):
                    si=str(i)
                    d = dst_pk+", "+ str(dst_keys[i])  #str(ti.xcom_pull( key= dst_pk+">"+si, task_ids='assert' ))
                    d = d+", "+ str(dst_vals[i])  #str(ti.xcom_pull( key= dst_pk+"."+si, task_ids='assert' ))
                    o = org_pk+", "+ str(org_keys[i])  #str(ti.xcom_pull( key= org_pk+">"+si, task_ids='assert' ))
                    o = o+", "+ str(org_vals[i])  #str(ti.xcom_pull( key= org_pk+"."+si, task_ids='assert' ))
                    a = str(do_asrts[i])  #str(ti.xcom_pull( key= "a."+si, task_ids='assert' ))

                    value = d+", "+o+", "+a+","+t+", '"+s+"'"
                    #print(value)

                    values=values+"(2, " + value + ")" #todo: replace 2 with the test_type_id
                    if i < end-1:
                        values=values+", "

                sql = "\"insert into table fact_test values "+values+"\""
                __class__.save_test_with_ssh_hive(sql)

        else:
            print('nothing to save')
            raise AirflowException(f"error saving test: data is None")
