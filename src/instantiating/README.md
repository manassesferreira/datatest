DISCLAIMER
as instanciações devem ser feitas após a construção das imagens
ver pasta building

- passo0
cd src/instantiating
su

#kylin airflow superset

- passo1
docker-compose up airflow-init

- passo2
docker-compose up

#KYLIN

- passo3
aguardar por volta de 5 minutos após passo2
localhost:8084/kylin ADMIN:KYLIN
    clicar no + 'Add Project'
       Project Name: datatest
    Submit

- passo4
rodar script de criação do modelo dimensional datatest no kylin

cd ../helpers
docker cp DATATEST/create.sh instantiating_kylin_1:/home/admin/
docker cp DATATEST/create_datatest_tables.sql instantiating_kylin_1:/home/admin/apache-kylin-4.0.0-bin-spark2/sample_cube/
docker cp DATATEST/DATATEST.DIM_ENTITY.csv instantiating_kylin_1:/home/admin/apache-kylin-4.0.0-bin-spark2/sample_cube/data/
docker cp DATATEST/DATATEST.DIM_TIME.csv instantiating_kylin_1:/home/admin/apache-kylin-4.0.0-bin-spark2/sample_cube/data/
docker cp DATATEST/DATATEST.DIM_TYPE.csv instantiating_kylin_1:/home/admin/apache-kylin-4.0.0-bin-spark2/sample_cube/data/
docker cp DATATEST/DATATEST.FACT_TEST.csv instantiating_kylin_1:/home/admin/apache-kylin-4.0.0-bin-spark2/sample_cube/data/
docker exec instantiating_kylin_1 /home/admin/create.sh

aguarde, o último comando demora cera de 5 minutos

- passo5
localhost:8084/kylin ADMIN:KYLIN
    "system reload metadata"
    click Web UI => System Tab => Reload Metadata to take effect
        Are you sure to reload metadata and clean cache? 
        YES

- passo6
localhost:8084/kylin ADMIN:KYLIN
    "model > data source > load table from tree"
    aguarde para carregar a árvore
    clique em datatest, e em seguida em  datatest.dim_entity, datatest.dim_time, datatest.dim_type e datatest.fact_test 
    clique em Sync

- passo7
configurar acesso ssh 

docker exec -ti instantiating_kylin_1 /bin/bash
    a*) corrigir yum repo ?
        https://stackoverflow.com/questions/21396508/yumrepo-error-all-mirror-urls-are-not-using-ftp-https-or-file
        vi /etc/yum.repos.d/CentOS-Base.repo  mirror to vault

    b) instalar openssh-server 
        yum update
            y <enter>

            ( ... pode ir pegar um cafe ... )

        yum install openssh-server
            y <enter>

    c) setar senha root 
       passwd 

    d) cd /etc/ssh
       ssh-keygen -A
       /usr/sbin/sshd

    e*)iniciar service sshd start
        service sshd start
        systemctl start sshd
            ??? Failed to get D-Bus connection: Operation not permitted
            https://stackoverflow.com/questions/50393525/failed-to-get-d-bus-connection-operation-not-permitted

    exit

#SUPERSET

- passo8
conexão

http://localhost:8088/ admin:admin
    DATA > DATABASES
        + DATABASE
             Supported database: Other
             SQLAlchemy URI*  :  kylin://ADMIN:KYLIN@kylin:7070/datatest
        CONNECT
        Edit database 'Other'
             Display Name*: kylin
        FINISH


#AIRFLOW

- passo9
criar conexoes

docker exec -ti instantiating_airflow-webserver_1 /bin/bash
    su airflow
    #antes de executar os comandos ajustar as senhas e demais parâmetros
    airflow connections add 'oracle' --conn-uri 'oracle://usuario:senha@hostname:porta/esquema?sid=sid&dsn=hostname'
    airflow connections add 'kylin_ssh' --conn-uri  'ssh://root:senha@kylin:22'
    airflow connections add 'kylin' --conn-uri  'jdbc://ADMIN:KYLIN@kylin:7070/datatest' 
    exit
    exit

http://localhost:8080 airflow:airflow
    clicar em admin > connections (http://localhost:8080/connection/list/)
    editar kylin (clicar em Edit record)
         Connection URL : jdbc:kylin://kylin:7070/datatest
         Driver Class : org.apache.kylin.jdbc.Driver
         Driver Path : /opt/kylin/kylin-jdbc-4.0.0-alpha.jar
    Save

- passo10
cd src/instantiating

lembrar das 3 horas a mais por conta do utc 
    e ajustar start_date e end_date em 
dags/tst_dag.py 

retirar o TEST_ASSERT_DATE substituindo o número por aspas duplas ""
dags/fact_test.json 

para monitorar as execuções caompanhando alterações no arquivo
cat dags/fact_test.json | python -m json.tool | more


- passo11
    https://stackoverflow.com/questions/43606311/refreshing-dags-without-web-server-restart-apache-airflow

    docker exec -ti instantiating_airflow-webserver_1 /bin/bash
         vim airflow.cfg 

             min_file_process_interval = 0
             dag_dir_list_interval = 60

         exit
    docker restart instantiating_airflow-webserver_1
    docker restart instantiating_airflow-scheduler_1 
    docker restart instantiating_airflow-worker_1 




#SUPERSET

- passo12
visualizações

http://localhost:8088/ admin:admin
#ROSCA
    SQL LAB > EDITOR
       editar os seguintes campos:
            DATABASE : kylin
            SCHEMA : DATATEST
            See table schema : FACT_TEST
            Caixa texto para inserir consulta sql:
                SELECT TEST_ASSERT
                FROM DATATEST.FACT_TEST
                WHERE TEST_ASSERT_TIME_ID = 1635606027
        selecionar RUN
             avaliar que valores foram exibidos corretamente na parte de baixo da tela
        clicar em EXPLORE
            Save as new : rosca
        SAVE & EXPLORE

        editar os seguintes campos:
            visualization type : pie chart
            metric : count(test_assert)
        RUN
            avaliar que eo gráfico foi gerado conforme esperado
        SAVE
           Chart name : rosca
           Add to dashboard : datatest
        ADD
#HISTOGRAMA
   #todo
#DISPERSAO
    #todo
 
