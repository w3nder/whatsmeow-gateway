FROM golang:1.26 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/gateway ./cmd/gateway

FROM gcr.io/distroless/static-debian12

COPY --from=build /out/gateway /gateway

USER nonroot:nonroot

ENTRYPOINT ["/gateway"]
