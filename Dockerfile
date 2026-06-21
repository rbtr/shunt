# Cross-compile friendly: the Go stage runs on the BUILD platform and emits a
# static binary for the TARGET platform (no qemu); the runtime stage is just a
# COPY onto a git-bearing base (shunt shells out to git), so it needs no qemu
# either. Build for your runtime arch (e.g. arm64 or amd64) with:
#   podman build --platform linux/arm64 -t shunt:<tag> .
#   docker buildx build --platform linux/amd64,linux/arm64 -t shunt:<tag> .
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -o /shunt ./cmd/shunt

FROM docker.io/alpine/git:latest
COPY --from=build /shunt /usr/local/bin/shunt
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/shunt"]
