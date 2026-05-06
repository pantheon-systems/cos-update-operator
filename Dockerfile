FROM alpine:3.20 AS certs

RUN apk update && apk add --no-cache ca-certificates && update-ca-certificates

FROM scratch

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ARG cmd

COPY bin/${cmd} /bin/${cmd}
ENTRYPOINT ["/bin/${cmd}"]
