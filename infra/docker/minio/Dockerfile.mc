FROM cgr.dev/chainguard/minio-client:latest AS mc
FROM alpine:3.21
COPY --from=mc /usr/bin/mc /usr/bin/mc
ENTRYPOINT ["/bin/sh"]
