FROM alpine:3.21

RUN apk add --no-cache ca-certificates iptables ip6tables curl fuse-overlayfs

# Install Tailscale.
ARG TAILSCALE_VERSION=1.80.3
RUN curl -fsSL "https://pkgs.tailscale.com/stable/tailscale_${TAILSCALE_VERSION}_amd64.tgz" \
    | tar xzf - --strip-components=1 -C /usr/local/bin \
      "tailscale_${TAILSCALE_VERSION}_amd64/tailscale" \
      "tailscale_${TAILSCALE_VERSION}_amd64/tailscaled"

# Install K3s.
ARG K3S_VERSION=v1.31.5+k3s1
RUN curl -fsSL "https://github.com/k3s-io/k3s/releases/download/${K3S_VERSION}/k3s" \
    -o /usr/local/bin/k3s && chmod +x /usr/local/bin/k3s

# Create required directories.
RUN mkdir -p /var/lib/tailscale /var/run/tailscale /var/lib/rancher/k3s

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
