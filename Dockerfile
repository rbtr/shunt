# Cross-compile friendly: the Go stage runs on the BUILD platform and emits a
# static binary for the TARGET platform (no qemu). The runtime stage is a pinned
# git-bearing base because shunt intentionally uses native git for staging merges.
# Build for your runtime arch (e.g. arm64 or amd64) with:
#   podman build --platform linux/arm64 -t shunt:<tag> .
#   docker buildx build --platform linux/amd64,linux/arm64 -t shunt:<tag> .
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.25.12-alpine@sha256:56961d79ea8129efddcc0b8643fd8a5416b4e6228cfd477e3fd61deb2672c587 AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -o /shunt ./cmd/shunt

FROM docker.io/alpine/git:2.49.1@sha256:c0280cf9572316299b08544065d3bf35db65043d5e3963982ec50647d2746e26
COPY --from=build /shunt /usr/local/bin/shunt
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/shunt"]
