#!/usr/bin/env bash
set -eo pipefail

# Download official mcp-grafana binary based on OS and architecture
VERSION="v1.2.0"
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

if [ "$ARCH" = "x86_64" ]; then
    ARCH_SUFFIX="x86_64"
    ALT_ARCH="x64"
elif [ "$ARCH" = "arm64" ] || [ "$ARCH" = "aarch64" ]; then
    ARCH_SUFFIX="arm64"
    ALT_ARCH="arm64"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi

mkdir -p bin

if [ "$OS" = "darwin" ]; then
    TARBALL="mcp-grafana_Darwin_${ARCH_SUFFIX}.tar.gz"
elif [ "$OS" = "linux" ]; then
    TARBALL="mcp-grafana_Linux_${ARCH_SUFFIX}.tar.gz"
else
    echo "Unsupported OS: $OS"
    exit 1
fi

URL="https://github.com/grafana/mcp-grafana/releases/download/${VERSION}/${TARBALL}"
echo "Downloading mcp-grafana ${VERSION} for ${OS}/${ARCH_SUFFIX} from ${URL}..."

curl -fsSL "$URL" | tar -xz -C bin/
chmod +x bin/mcp-grafana

echo "mcp-grafana successfully installed to bin/mcp-grafana"
./bin/mcp-grafana -version || true
