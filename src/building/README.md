DISCLAIMER
As imagens aqui construídas são para o ambiente de desenvolvimento.
Para colocar em produção cabem otimizações.

cd src/building

#AIRFLOW

- passo1
git clone https://github.com/apache/airflow.git

- passo2
<DOCKERFILE: editar para inserir 1) instant client Oracle e 2) driver jdbc Kylin>
vim airflow/Dockerfile

- passo2.0
"""
RUN pip install apache-airflow-providers-ssh

RUN pip install cx_Oracle apache-airflow-providers-oracle apache-airflow-providers-jdbc sqlalchemy pandas psycopg2-binary kafka-python
"""

Inserir as linhas acima (entre os """'s) antes de
LABEL. orgapache.airflow.distro="debian" \

e depois de
USER ${AIRFLOW_UID}

- passo2.1
adicionar libaio1, wget em RUNTIME_APT_DEPS

""""
RUN mkdir /opt/java
WORKDIR /opt/java
ENV JAVA_HOME /opt/java/jdk1.8.0_141
RUN wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u141-b15/336fa29ff2bb4ef291e347e091f7f4a7/jdk-8u141-linux-x64.tar.gz" \
    && tar -zxvf /opt/java/jdk-8u141-linux-x64.tar.gz \
    && rm -f /opt/java/jdk-8u141-linux-x64.tar.gz

COPY dags/ /opt/airflow/dags/
COPY kylin/ /opt/kylin
COPY oracle/ /opt/oracle
ENV LD_LIBRARY_PATH /opt/oracle/instantclient_19_6:${LD_LIBRARY_PATH}
ENV PATH /opt/oracle/instantclient_19_6:${PATH}
RUN echo "/opt/oracle/instantclient_19_6" > /etc/ld.so.conf.d/oracle-instantclient.conf
RUN ldconfig
"""

- passo2.2
Inserir as linhas acima (entre os """'s) antes de
# Make /etc/passwd root-group-writeable so that user can be dynamically added by OpenShift

e depois de 
COPY --chown=airflow:root scripts/in_container/prod/clean-logs.sh /clean-logs

nota: precisei remover '.dev0' de uma variável de versão 
ARG AIRFLOW_VERSION="2.2.0"

pois https://raw.githubusercontent.com/apache/airflow/constraints-2.2.0.dev0/constraints-3.6.txt 
não existia...

- passo2.3
copiar diretórios dags kylin oracle
cp -rp dags airflow/
cp -rp kylin airflow/
cp -rp oracle airflow/

- passo2.4
editar .dockerignore

após 
!scripts/docker 

acrescentar
!kylin
!oracle

- passo3
cd airflow

- passo4
docker build -t apache/airflow .

aguardar pelo menos 20 minutos

- passo5
cd..

#SUPERSET

- passo6
git clone https://github.com/apache/superset.git

- passo7
<DOCKERFILE: editar para inserir em requeriments/local.txt) kylinpy>

echo "kylinpy" >> superset/requirements/local.txt

- passo8
cd superset

- passo9
docker build -t apache/superset .

aguardar pelo menos 10 minutos

- passo10
cd..

#KYLIN

- passo11
docker pull apachekylin/apache-kylin-standalone:4.0.0
