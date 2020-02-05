FROM openjdk:8-jre

ADD target/jmetalsp-externalsource-2.1-SNAPSHOT-jar-with-dependencies.jar /opt/jmetalsp-externalsource-2.1-SNAPSHOT-jar-with-dependencies.jar

RUN mkdir -p /data/outputTraffic

CMD ["java", "-cp", "/opt/jmetalsp-externalsource-2.1-SNAPSHOT-jar-with-dependencies.jar", "org.uma.jmetalsp.externalsources.TrafficSource", "/data/outputTraffic", "60", "csv"]
