FROM openjdk:11
EXPOSE 8080
ADD target/*.jar app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
#huy8895/kafka-publisher:latest
#docker run -d --name demo_kafka_consumer -e SERVER_PORT=8088 --network dev_default -e KAFKA_BOOTSTRAP_SERVERS=kafka-0:9092,kafka-1:9092,kafka-2:9092 huy8895/kafka-publisher:latest