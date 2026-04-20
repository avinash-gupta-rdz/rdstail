# Multi-stage build producing a tiny static binary image.
# Build:
#   docker build -f deploy/Dockerfile -t rdstail:dev .
# Run:
#   docker run --rm -v $PWD/examples:/etc/rds -p 9090:9090 \
#     rdstail:dev run -c /etc/rds/config.yaml
ARG GO_VERSION=1.22

FROM golang:${GO_VERSION}-alpine AS builder
ENV CGO_ENABLED=0 GOFLAGS=-mod=readonly
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
RUN go build \
      -trimpath \
      -ldflags "-s -w \
        -X github.com/avinash-gupta-rdz/rdstail/internal/cli.version=${VERSION} \
        -X github.com/avinash-gupta-rdz/rdstail/internal/cli.commit=${COMMIT} \
        -X github.com/avinash-gupta-rdz/rdstail/internal/cli.buildDate=${BUILD_DATE}" \
      -o /out/rdstail \
      ./cmd/rdstail

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/rdstail /usr/local/bin/rdstail
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/rdstail"]
CMD ["run", "-c", "/etc/rdstail/config.yaml"]
