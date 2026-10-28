# syntax=docker/dockerfile:1

# The meshub backend. The SQLite driver is pure Go, so the binary is static
# and the image holds nothing else.
#
#   docker build -t meshub --build-arg VERSION=$(git describe --always --dirty) .
#   docker run -v meshub-data:/data -p 8080:8080 -e MESHUB_MQTT_BROKER=tcp://broker:1883 meshub

FROM golang:1.27-alpine AS build
WORKDIR /src
# A local go.work may point at sibling checkouts outside the build context;
# build from the modules' own requirements. The server module uses the root
# module (for proto/) from this checkout.
ENV CGO_ENABLED=0 GOWORK=off
COPY go.mod go.sum ./
COPY server/go.mod server/go.sum ./server/
RUN --mount=type=cache,target=/go/pkg/mod cd server && go mod download
COPY proto ./proto
COPY server ./server
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
	cd server && go build -trimpath -buildvcs=false -ldflags="-s -w -X main.version=${VERSION}" -o /out/meshub ./cmd/meshub
RUN mkdir /out/data

FROM scratch
# CA certificates for brokers reached over TLS (ssl://, tls://, wss://).
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/meshub /meshub
# /data holds the database, and with it the hub's private key: keep it on a
# volume and back it up.
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
ENV MESHUB_DB_PATH=/data/meshub.db \
	MESHUB_HTTP_ADDR=:8080
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s CMD ["/meshub", "healthcheck"]
ENTRYPOINT ["/meshub"]
CMD ["serve"]
