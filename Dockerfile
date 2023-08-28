FROM openjdk:11-jre-alpine
ENV MAIN_CLASS=com.azure.kusto.mastodon.consumer.MastodonConsumer
WORKDIR /app
COPY target/mastadon-consumer-1.0.jar /app
RUN ls -l /app
CMD ["java -jar /app/mastadon-consumer-1.0.jar"]