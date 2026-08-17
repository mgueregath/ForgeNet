# Sirve el ejemplo taca-taca como referencia de deploy — cualquier otro
# juego construido sobre go/networkcore se dockeriza igual: cambiar el
# ./ejemplo-tacataca de la etapa de build por el binario propio.

FROM golang:1.26-alpine AS builder
WORKDIR /src

COPY go/go.mod go/go.sum ./
RUN go mod download

COPY go/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./ejemplo-tacataca

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/server /server
# La página de prueba del navegador es opcional en producción (ver
# WEB_DIR en main.go): si no se copia, el server sigue funcionando y solo
# no sirve esa página de prueba.
COPY web/ /web/
ENV WEB_DIR=/web

EXPOSE 9999/udp
EXPOSE 9443/udp
EXPOSE 8080/tcp

ENTRYPOINT ["/server"]
