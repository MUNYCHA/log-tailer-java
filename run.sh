#!/usr/bin/env bash
#
# Launches log-tailer with a bounded JVM so it can never crowd out other
# processes on a shared host. A bare `java -jar` defaults to a max heap of
# 25% of the machine's RAM and a GC that does not return memory to the OS,
# which lets the process slowly starve the host. The flags below cap that.
#
# Override any default by exporting the variable before running, e.g.:
#   JVM_MAX_HEAP=512m ./run.sh --config=/etc/log-tailer/config.json

set -euo pipefail

JAR="${JAR:-$(dirname "$0")/target/log-tailer.jar}"
JVM_MIN_HEAP="${JVM_MIN_HEAP:-64m}"
JVM_MAX_HEAP="${JVM_MAX_HEAP:-256m}"

if [[ ! -f "${JAR}" ]]; then
  echo "Error: jar not found at '${JAR}'." >&2
  echo "Build it first with 'mvn clean package', or set JAR=/path/to/log-tailer.jar." >&2
  exit 1
fi

exec java \
  -Xms"${JVM_MIN_HEAP}" -Xmx"${JVM_MAX_HEAP}" \
  -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
  -XX:+ExitOnOutOfMemoryError \
  -jar "${JAR}" "$@"
