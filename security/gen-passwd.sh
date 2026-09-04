set -euo pipefail

USER_NAME="${MQTT_USERNAME:-bushfire}"
PASS="${MQTT_PASSWORD:-}"

if [ -z "$PASS" ]; then
  echo "error: MQTT_PASSWORD is not set. Export it (or add it to .env and source it)." >&2
  exit 1
fi

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$REPO/mqtt/passwd"

echo "[gen-passwd] creating $OUT for user '$USER_NAME'"

# -c creates/overwrites the file; -b takes the password on the command line.
# The mqtt/ directory is mounted at /export inside the throwaway container.
docker run --rm -v "$REPO/mqtt:/export" eclipse-mosquitto:latest \
  mosquitto_passwd -c -b /export/passwd "$USER_NAME" "$PASS"

echo "[gen-passwd] done. The secured broker (mosquitto.conf) will use this file."

# ---------------------------------------------------------------------------
# Equivalent WITHOUT Docker, if mosquitto_passwd is installed locally:
#
#   mosquitto_passwd -c -b mqtt/passwd "$MQTT_USERNAME" "$MQTT_PASSWORD"
#
# To ADD another user to an existing file, drop the -c flag:
#
#   mosquitto_passwd -b mqtt/passwd another_user another_pass
# ---------------------------------------------------------------------------
