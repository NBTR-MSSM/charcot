FROM eclipse-temurin:17.0.4_8-jre

# FIXME: Why did I make STAGE and argument that then gets handed off to an ENV? Can't it just be an ENV directly, and thus turn it into
#  a parameter available at runtime??? Because otherwise (under the current setup) , the stage is just baked into the image, yet the image
#  name is not reflective of the stage either, see /Users/jmquij0106/git/charcot/fulfillment/script/charcot-fulfillment.sh
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