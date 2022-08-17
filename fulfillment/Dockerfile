FROM eclipse-temurin:17.0.4_8-jre

ARG STAGE=debug
ENV SPRING_PROFILE=$STAGE
ENV AWS_PROFILE=mssm
ENV IS_LOCAL=false

COPY target/fulfillment-0.0.1-SNAPSHOT.jar app.jar

# Install dependencies
RUN apt update
RUN apt install -y zip

EXPOSE 80

ENTRYPOINT java -Dcharcot.is.local=$IS_LOCAL -Dspring.profiles.active=$SPRING_PROFILE -jar app.jar