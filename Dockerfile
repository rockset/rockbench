FROM golang:alpine AS builder

RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

WORKDIR /app

COPY . /app/

RUN CGO_ENABLED=0 go build

FROM alpine
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/rockbench /

CMD ["/rockbench"]
