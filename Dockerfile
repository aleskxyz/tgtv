FROM golang:1.25-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tgtv ./cmd/tgtv

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /tgtv /usr/local/bin/tgtv

ENV SESSION_DIR=/data/session \
    CONFIG_DIR=/data/config \
    HTTP_HOST=0.0.0.0 \
    HTTP_PORT=8090

EXPOSE 8090

CMD ["tgtv", "serve"]
