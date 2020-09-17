FROM golang:alpine AS builder

RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

WORKDIR /app

COPY . /app/

RUN CGO_ENABLED=0 go build

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/generator /app

ENTRYPOINT ["/app/generator"]
