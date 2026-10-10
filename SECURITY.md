# Security policy

yscale's central server holds cloud-provider credentials and Tailscale OAuth
secrets, and mints mesh auth keys and kubelet bootstrap tokens. Treat every
bearer token (`YSCALE_TOKEN`, the dev tenant token logged at startup) and every
env secret as sensitive. Please take vulnerabilities in this codebase seriously
— we do.

## Reporting a vulnerability

**Do not open a public issue for a security vulnerability.**

Report privately via **GitHub Security Advisories**: the repository's
*Security* tab → *Report a vulnerability*. If that is unavailable, email the
maintainer listed in the repository profile with subject `[yscale security]`.

Please include: the affected component (central / agent / burst image / Helm
chart), a reproduction or proof of concept, and the impact as you understand
it.

## What to expect

This is a small-team project. We aim to acknowledge reports within **72
hours** and to ship or coordinate a fix promptly for anything that affects
credential exposure, tenant/workload isolation, or the mesh join path. We are
happy to credit reporters in release notes (or keep you anonymous — your
call).

## Scope notes

- **In scope:** the `central`, `agent`, and `burst` code paths, the Helm
  charts, and the published container images built from this repo.
- **Deployment reality check:** the OSS edition is designed for a
  single-operator setup. central's API should not be exposed to the public
  internet without TLS and network-level restriction; the dev tenant token is
  randomized per seed, but anything that can read central's logs or env can
  act as that tenant.
- **Out of scope:** vulnerabilities in the upstream clouds (Fly.io, Linode,
  AWS), Tailscale itself, or Kubernetes — report those upstream (we'll gladly
  help route them).
