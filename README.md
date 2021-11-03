# datatest
Os testes automáticos em armazém (com volumes gigantescos) de dados 
podem ser usados para detecção de não-conformidades
durante o processo de extração, transformação e carga.
O presente trabalho apresenta uma implementação no ecossistema Apache
(Airflow, Kylin e Superset) de testes centrados em dados.
As medidas e variáveis envolvidas nos testes são persistidas em
um modelo dimensional que permite a geração de relatórios históricos.
O principal teste implementado compara entidades do modelo
através de consultas SQL para contagem de linhas.
Os testes são automatizados pela aplicação Apache Airflow, 
os resultados dos testes são persistidos no ambiente Apache Kylin e a análise dos mesmos é facilitada por painéis/gráficos 
gerados no software Apache Superset.
