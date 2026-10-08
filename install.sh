#!/bin/sh
# rdstail installer: downloads a release binary, verifies its SHA-256 against
# the release's checksums.txt, and installs it.
#
#   curl -fsSL https://raw.githubusercontent.com/avinash-gupta-rdz/rdstail/main/install.sh | sh
#
# Environment:
#   RDSTAIL_VERSION      release tag to install, e.g. v0.3.0 (default: latest)
#   RDSTAIL_INSTALL_DIR  install directory (default: /usr/local/bin, or
#                        ~/.local/bin when /usr/local/bin isn't writable
#                        and sudo isn't available)
set -eu

REPO="avinash-gupta-rdz/rdstail"
VERSION="${RDSTAIL_VERSION:-latest}"

err() { echo "rdstail-install: $*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || err "required command not found: $1"; }
need uname
need tar
need mktemp

if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL -o "$2" "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -qO "$2" "$1"; }
else
  err "need curl or wget"
fi

if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
else
  err "need sha256sum or shasum to verify the download"
fi

case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=darwin ;;
  *) err "unsupported OS: $(uname -s) (linux and darwin only)" ;;
esac

case "$(uname -m)" in
  x86_64 | amd64) arch=amd64 ;;
  arm64 | aarch64) arch=arm64 ;;
  *) err "unsupported architecture: $(uname -m) (amd64 and arm64 only)" ;;
esac

if [ "$VERSION" = "latest" ]; then
  base="https://github.com/${REPO}/releases/latest/download"
else
  base="https://github.com/${REPO}/releases/download/${VERSION}"
fi
archive="rdstail_${os}_${arch}.tar.gz"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

echo "Downloading ${archive} (${VERSION})..."
fetch "${base}/${archive}" "${tmp}/${archive}" || err "download failed: ${base}/${archive}"
fetch "${base}/checksums.txt" "${tmp}/checksums.txt" || err "download failed: ${base}/checksums.txt"

want="$(awk -v f="$archive" '$2 == f {print $1}' "${tmp}/checksums.txt")"
[ -n "$want" ] || err "${archive} not listed in checksums.txt"
got="$(sha256 "${tmp}/${archive}")"
[ "$want" = "$got" ] || err "checksum mismatch for ${archive}: want ${want}, got ${got}"
echo "Checksum verified."

tar -xzf "${tmp}/${archive}" -C "$tmp" rdstail

dir="${RDSTAIL_INSTALL_DIR:-}"
sudo=""
if [ -z "$dir" ]; then
  dir=/usr/local/bin
  if [ ! -w "$dir" ]; then
    if command -v sudo >/dev/null 2>&1; then
      sudo=sudo
    else
      dir="${HOME}/.local/bin"
    fi
  fi
fi

$sudo mkdir -p "$dir"
$sudo install -m 0755 "${tmp}/rdstail" "${dir}/rdstail"

echo "Installed rdstail to ${dir}/rdstail"
case ":${PATH}:" in
  *":${dir}:"*) "${dir}/rdstail" version ;;
  *) echo "Note: ${dir} is not on your PATH; add it to run rdstail directly." ;;
esac
