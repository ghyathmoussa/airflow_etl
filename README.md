# Describtion
Connection Docker with airflow, this system that take data from tow json files and upload this data to MySQL database by using the ETL 
technique.
This is a simple system of ETL using docker and airflow.
# Installation
clone the project folder using:
```
git clone https://github.com/ghyathmoussa/airflow_etl.git
```
then run the command that start the airflow in docker
```
docker-compose up airflow-init
```

to start the project run
```
docker-compose up
```

the go to (127.0.0.1:8080) which run the airflow webserver on port 8080 and see all dags