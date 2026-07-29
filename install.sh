#!/bin/sh
# rdstail installer — downloads the release archive for this OS/arch, verifies
# its SHA-256 against the release's checksums.txt, and installs the binary.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/avinash-gupta-rdz/rdstail/main/install.sh | sh
#
# Environment overrides:
#   RDSTAIL_VERSION      release tag to install, e.g. v0.2.0 (default: latest)
#   RDSTAIL_INSTALL_DIR  target directory (default: /usr/local/bin)
set -eu

REPO="avinash-gupta-rdz/rdstail"
VERSION="${RDSTAIL_VERSION:-latest}"
INSTALL_DIR="${RDSTAIL_INSTALL_DIR:-/usr/local/bin}"

err() { printf 'install.sh: %s\n' "$*" >&2; exit 1; }

command -v curl >/dev/null 2>&1 || err "curl is required"
command -v tar >/dev/null 2>&1 || err "tar is required"

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$os" in
  linux|darwin) ;;
  *) err "unsupported OS: $os (linux and darwin only — see releases page)" ;;
esac

arch=$(uname -m)
case "$arch" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) err "unsupported architecture: $arch (amd64 and arm64 only)" ;;
esac

# Resolve "latest" to a concrete tag via the releases redirect (no API token,
# no rate-limit surprises).
if [ "$VERSION" = "latest" ]; then
  VERSION=$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
    "https://github.com/${REPO}/releases/latest") || err "could not reach github.com"
  VERSION=${VERSION##*/}
  case "$VERSION" in
    v*) ;;
    *) err "no published release found — is the first release tagged yet?" ;;
  esac
fi

# Archive names use the version without the leading v (goreleaser default).
bare_version=${VERSION#v}
archive="rdstail_${bare_version}_${os}_${arch}.tar.gz"
base_url="https://github.com/${REPO}/releases/download/${VERSION}"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM

printf 'Downloading %s %s (%s/%s)...\n' "$REPO" "$VERSION" "$os" "$arch"
curl -fsSL -o "$tmp/$archive" "$base_url/$archive" \
  || err "download failed: $base_url/$archive"
curl -fsSL -o "$tmp/checksums.txt" "$base_url/checksums.txt" \
  || err "download failed: $base_url/checksums.txt"

printf 'Verifying checksum...\n'
grep " ${archive}\$" "$tmp/checksums.txt" > "$tmp/expected.txt" \
  || err "no checksum entry for $archive in checksums.txt"
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$tmp" && sha256sum -c expected.txt >/dev/null) \
    || err "checksum verification FAILED for $archive — aborting"
elif command -v shasum >/dev/null 2>&1; then
  (cd "$tmp" && shasum -a 256 -c expected.txt >/dev/null) \
    || err "checksum verification FAILED for $archive — aborting"
else
  err "need sha256sum or shasum to verify the download"
fi

tar -xzf "$tmp/$archive" -C "$tmp" rdstail

if [ -w "$INSTALL_DIR" ]; then
  install -m 0755 "$tmp/rdstail" "$INSTALL_DIR/rdstail"
elif command -v sudo >/dev/null 2>&1; then
  printf 'Installing to %s (needs sudo)...\n' "$INSTALL_DIR"
  sudo install -m 0755 "$tmp/rdstail" "$INSTALL_DIR/rdstail"
else
  err "$INSTALL_DIR is not writable and sudo is unavailable — rerun with RDSTAIL_INSTALL_DIR=\$HOME/.local/bin"
fi

printf 'Installed: %s\n' "$("$INSTALL_DIR/rdstail" version)"
printf 'Get started: rdstail init   (or: rdstail tail -i <instance> --region <region>)\n'
