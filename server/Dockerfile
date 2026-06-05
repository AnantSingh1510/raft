FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/raft-server ./cmd/raft-server

FROM alpine:3.22

RUN adduser -D -H raft
USER raft
WORKDIR /app
COPY --from=build /out/raft-server /usr/local/bin/raft-server

ENV RAFT_ADDR=:8080
EXPOSE 8080

ENTRYPOINT ["raft-server"]

