#!/usr/bin/env bash
# Download the AWS SDK bundle required by Hive Metastore (too large for GitHub).
set -euo pipefail

VERSION="1.12.367"
JAR_NAME="aws-java-sdk-bundle-${VERSION}.jar"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="${SCRIPT_DIR}/../hive-jars"
DEST="${JARS_DIR}/${JAR_NAME}"
URL="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${VERSION}/${JAR_NAME}"

mkdir -p "$JARS_DIR"

if [ -f "$DEST" ]; then
  echo "Hive jar already present: $JAR_NAME"
  exit 0
fi

echo "Downloading $JAR_NAME (~280 MB)..."
curl -fL "$URL" -o "$DEST"
echo "Saved to $DEST"
