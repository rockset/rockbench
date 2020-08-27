FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git \
    python3 \
    py-pip && \
    pip install --upgrade awscli==1.14.5

WORKDIR /app

COPY . /app/

RUN CGO_ENABLED=0 go build -o /app/main .

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/main /app/main

ENTRYPOINT ["/app/main"]
