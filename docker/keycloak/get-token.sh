#!/usr/bin/env bash
# A client-credentials token from the local Keycloak, for the HIE API or for HFS.
#
#   ./get-token.sh                      # hie-admin-client: everything on the HIE API
#   ./get-token.sh hie-operator-client  # sources and rules, no code
#   ./get-token.sh hie-monitor-client   # read only
#   ./get-token.sh hfs-backend-client   # what the engine itself uses towards HFS
#
# The access token goes to stdout, everything else to stderr:
#   export TOKEN=$(./get-token.sh)

set -euo pipefail

KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8180}"
REALM="${REALM:-fhir}"
CLIENT_ID="${1:-hie-admin-client}"

case "${CLIENT_ID}" in
  hie-admin-client)    CLIENT_SECRET="${CLIENT_SECRET:-hie-admin-secret}" ;;
  hie-operator-client) CLIENT_SECRET="${CLIENT_SECRET:-hie-operator-secret}" ;;
  hie-monitor-client)  CLIENT_SECRET="${CLIENT_SECRET:-hie-monitor-secret}" ;;
  hfs-backend-client)  CLIENT_SECRET="${CLIENT_SECRET:-hfs-backend-secret}" ;;
  hfs-readonly-client) CLIENT_SECRET="${CLIENT_SECRET:-hfs-readonly-secret}" ;;
  *)                   CLIENT_SECRET="${CLIENT_SECRET:?CLIENT_SECRET must be set for ${CLIENT_ID}}" ;;
esac

TOKEN_ENDPOINT="${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token"
echo "Requesting a token from ${TOKEN_ENDPOINT} for ${CLIENT_ID}" >&2

RESPONSE=$(curl -sf -X POST "${TOKEN_ENDPOINT}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  --data-urlencode "client_id=${CLIENT_ID}" \
  --data-urlencode "client_secret=${CLIENT_SECRET}")

field() {
  echo "${RESPONSE}" | node -e "let d='';process.stdin.on('data',c=>d+=c).on('end',()=>process.stdout.write(String(JSON.parse(d)['$1']??'')))"
}

echo "Expires in $(field expires_in) s; scope: $(field scope)" >&2
field access_token
echo
