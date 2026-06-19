#!/usr/bin/env bash
# setup-certs.sh
#
# Generate a self-signed CA + per-node (and client) certificates for use with
# ghostunnel mTLS sidecars wrapping KayaDB nodes.
#
# This is intended for demos and testing only. For production, use a proper PKI
# (e.g. Vault, cert-manager, or your internal CA) and short-lived certs.
#
# Usage:
#   CERTS_DIR=./certs ./scripts/mtls-sidecar/setup-certs.sh
#
# Produces in $CERTS_DIR/ :
#   ca.crt, ca.key
#   node1.{crt,key,p12}, node2..., node3...
#   client.{crt,key,p12}
#
# The .p12 files have empty passphrase (demo only; ghostunnel --keystore accepts it).
# CNs: nodeN.kaya.local and admin-client.kaya.local
set -euo pipefail

# Prevent Git Bash / MSYS from mangling -subj "/C=..." paths on Windows
export MSYS_NO_PATHCONV=1

CERTS_DIR="${CERTS_DIR:-certs}"
mkdir -p "$CERTS_DIR"
pushd "$CERTS_DIR" > /dev/null

echo "==> Generating mTLS certificates for KayaDB ghostunnel demo in $(pwd)"

# 1. CA (self-signed)
openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt \
  -subj "/C=US/ST=Demo/L=Local/O=KayaDB Demo/OU=Sidecar/CN=KayaDB Demo Root CA" \
  2>/dev/null
echo "  + ca.crt / ca.key (10 year validity for demo)"

# 2. Per-node server/client certs (used by both server ghostunnels and as client identity when proxying)
for i in 1 2 3; do
  NODE="node${i}"
  CN="${NODE}.kaya.local"

  openssl genrsa -out "${NODE}.key" 4096 2>/dev/null
  openssl req -new -key "${NODE}.key" -out "${NODE}.csr" \
    -subj "/C=US/ST=Demo/L=Local/O=KayaDB Demo/OU=Node/CN=${CN}" \
    2>/dev/null
  openssl x509 -req -in "${NODE}.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "${NODE}.crt" -days 365 -sha256 2>/dev/null

  # PKCS12 for ghostunnel (empty password for demo convenience)
  openssl pkcs12 -export -out "${NODE}.p12" \
    -inkey "${NODE}.key" -in "${NODE}.crt" -certfile ca.crt \
    -passout pass: 2>/dev/null

  rm -f "${NODE}.csr"
  echo "  + ${NODE}.crt / ${NODE}.key / ${NODE}.p12 (CN=${CN})"
done

# 3. Separate admin/client cert for external tools (kayactl via client-side ghostunnel, etc.)
openssl genrsa -out client.key 4096 2>/dev/null
openssl req -new -key client.key -out client.csr \
  -subj "/C=US/ST=Demo/L=Local/O=KayaDB Demo/OU=Client/CN=admin-client.kaya.local" \
  2>/dev/null
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days 365 -sha256 2>/dev/null

openssl pkcs12 -export -out client.p12 \
  -inkey client.key -in client.crt -certfile ca.crt \
  -passout pass: 2>/dev/null

rm -f client.csr
echo "  + client.crt / client.key / client.p12 (CN=admin-client.kaya.local)"

# Cleanup serial if present
rm -f ca.srl

popd > /dev/null

echo ""
echo "==> Certs ready in $CERTS_DIR/"
echo "    Use with ghostunnel:"
echo "      server: --keystore $CERTS_DIR/nodeN.p12 --cacert $CERTS_DIR/ca.crt"
echo "      client: --keystore $CERTS_DIR/client.p12 --cacert $CERTS_DIR/ca.crt"
echo ""
echo "WARNING: These are self-signed demo certs. Do NOT use in production."
echo "         Rotate frequently and protect private keys."
