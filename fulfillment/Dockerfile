FROM eclipse-temurin:17.0.4_8-jre

# The STAGE needs to be a build-time argument because at build time is the only chance
# we get to specify the stage, a constraint created by aws-cdk-lib ContainerImage.fromAsset(), which accepts DockerImageAssetOptions.buildArgs(),
# but nothing to override ENV's at runtime. In other words, the stage is baked into the image produced by this Docker file.
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
