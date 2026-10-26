# Build a static binary, then ship it alone in a minimal image.
FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mapreduce ./cmd/mapreduce

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/mapreduce /usr/local/bin/mapreduce
ENTRYPOINT ["/usr/local/bin/mapreduce"]
