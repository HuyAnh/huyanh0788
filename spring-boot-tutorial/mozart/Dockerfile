#Environment layer
FROM maven:3-jdk-8-alpine 
ADD . /home/mozart-workflow
WORKDIR /home/mozart-workflow
RUN mvn clean install

#Build layer
RUN chmod -R 755 ./target/mozart-workflow-0.0.1-SNAPSHOT-spring-boot.jar
CMD java -jar ./target/mozart-workflow-0.0.1-SNAPSHOT-spring-boot.jar
