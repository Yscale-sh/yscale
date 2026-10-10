# yscale-website — Vite/React static site served by a small Go server that
# also captures waitlist signups (POST /api/signup -> /data/signups.jsonl).
# Built by the platform image-builder (see deploy.yaml build block).
FROM node:22-alpine AS web
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM golang:1.25-alpine AS server
WORKDIR /src
COPY server/go.mod server/go.sum ./
RUN go mod download
COPY server/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/website-server .

FROM alpine:3.20
RUN adduser -D -u 10001 web && mkdir -p /data && chown web:web /data
COPY --from=server /out/website-server /usr/local/bin/website-server
COPY --from=web /app/dist /srv/dist
ENV STATIC_DIR=/srv/dist
USER web
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/website-server"]
