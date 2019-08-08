# RabbitMQ VS Kafka

This repo contains the basic examples of rabbitMQ and demonstrate the usage of both message broker.

## You don't need to install RabbitMQ & Kafka. There is a infra folder inside the respective module. 
## Which has the docker compose files to start and stop the RabbitMQ & Kafka.
## Go inside the infra folder and execute the below commands.

#### To start 
``` 
docker-compose up --build
```
#### To access the RabbitMQ admin management tool. Access the URL localhost:15672. Username and Password is guest
#### To stop the docker container: CTL + C
#### Follow the README.md file in kafka module to setup the topics. 
