# Build with an extracted, checksum-verified Kafka distribution as the context.
# BASE_IMAGE must be a trusted, already available Java 17+ Linux image.
ARG BASE_IMAGE=eclipse-temurin:21-jre
FROM ${BASE_IMAGE}
USER root
COPY kafka/ /opt/hackathon-kafka/
COPY broker-entrypoint.sh /opt/hackathon-entrypoint.sh
RUN mkdir -p /var/lib/hackathon-kafka && \
    chown -R 1001:0 /var/lib/hackathon-kafka /opt/hackathon-kafka && \
    chmod 755 /opt/hackathon-entrypoint.sh
USER 1001
ENTRYPOINT ["/opt/hackathon-entrypoint.sh"]
