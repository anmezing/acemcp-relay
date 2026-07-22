FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -o lce-relay .

FROM alpine:3
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /app/lce-relay .
EXPOSE 3009
CMD ["./lce-relay"]
