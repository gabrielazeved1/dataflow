FROM alpine:3.21

RUN apk add --no-cache curl \
 && curl -fsSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /usr/bin/minio \
 && chmod +x /usr/bin/minio \
 && apk del curl

EXPOSE 9000 9001

ENTRYPOINT ["/usr/bin/minio"]
