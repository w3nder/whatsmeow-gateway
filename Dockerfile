FROM golang:1.26 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/gateway ./cmd/gateway

RUN cat /etc/ssl/certs/ca-certificates.crt tls/clickmassax-ca.crt > /out/ca-bundle.crt

FROM gcr.io/distroless/static-debian12

COPY --from=build /out/gateway /gateway
COPY --from=build /out/ca-bundle.crt /etc/clickmassax/ca-bundle.crt

ENV SSL_CERT_FILE=/etc/clickmassax/ca-bundle.crt

USER nonroot:nonroot

ENTRYPOINT ["/gateway"]
