set -euo pipefail

# Resolve the directory this script lives in, so it works from any CWD.
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAYS="${DAYS:-825}"

# Subject fields — cosmetic for a self-signed demo CA.
CA_SUBJ="/C=AU/ST=Victoria/O=Bushfire IoT/CN=Bushfire Demo Root CA"
BROKER_CN="mosquitto"

echo "[gen-certs] writing certificates to: $DIR"

# 1) Root CA: private key + self-signed certificate.
openssl genrsa -out "$DIR/ca.key" 4096
openssl req -x509 -new -nodes \
  -key "$DIR/ca.key" \
  -sha256 -days "$DAYS" \
  -subj "$CA_SUBJ" \
  -out "$DIR/ca.crt"

# 2) Broker key + CSR.
openssl genrsa -out "$DIR/broker.key" 4096
openssl req -new \
  -key "$DIR/broker.key" \
  -subj "/C=AU/ST=Victoria/O=Bushfire IoT/CN=${BROKER_CN}" \
  -out "$DIR/broker.csr"

# 3) Sign the broker CSR with the CA, adding SANs so hostname verification
#    passes for the compose service name and for local host connections.
cat > "$DIR/broker.ext" <<EOF
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:mosquitto, DNS:localhost, IP:127.0.0.1
EOF

openssl x509 -req \
  -in "$DIR/broker.csr" \
  -CA "$DIR/ca.crt" -CAkey "$DIR/ca.key" -CAcreateserial \
  -sha256 -days "$DAYS" \
  -extfile "$DIR/broker.ext" \
  -out "$DIR/broker.crt"

# 4) Tidy intermediate files; keep only the four artefacts the stack uses.
rm -f "$DIR/broker.csr" "$DIR/broker.ext" "$DIR/ca.srl"

# Broker/CA private keys must not be world-readable.
chmod 600 "$DIR/ca.key" "$DIR/broker.key" 2>/dev/null || true

echo "[gen-certs] done:"
echo "  CA cert     : $DIR/ca.crt"
echo "  broker cert : $DIR/broker.crt (SAN: mosquitto, localhost, 127.0.0.1)"
echo
echo "Next: set TLS_ENABLED=true and point MQTT clients at mqtts://<host>:8883"
echo "with MQTT_CA_FILE pointing at ca.crt. See the README Security section."
