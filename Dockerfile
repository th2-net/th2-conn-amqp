FROM gradle:6.7-jdk11 AS build
COPY ./ .
RUN gradle --no-daemon clean build dockerPrepare

FROM adoptopenjdk/openjdk11:alpine
WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service"]