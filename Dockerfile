FROM maven:3.8.2-openjdk-17 AS build
COPY ./ /build_dir
WORKDIR /build_dir
RUN --mount=type=cache,target=/root/.m2 mvn clean install
CMD java -jar /build_dir/target/*.jar
