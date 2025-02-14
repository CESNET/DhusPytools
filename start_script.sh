#!/bin/bash
set -e

NETRC_FILE="/root/.netrc"

# Verify that all necessary variables are set for the first block
if [ -z "$NETRC_MACHINE1" ] || [ -z "$NETRC_LOGIN1" ] || [ -z "$NETRC_PASSWORD1" ]; then
  echo "Pro první blok .netrc nejsou nastaveny všechny proměnné (NETRC_MACHINE1, NETRC_LOGIN1, NETRC_PASSWORD1)."
  exit 1
fi

# Verify that all necessary variables are set for the second block
if [ -z "$NETRC_MACHINE2" ] || [ -z "$NETRC_LOGIN2" ] || [ -z "$NETRC_PASSWORD2" ]; then
  echo "Pro druhý blok .netrc nejsou nastaveny všechny proměnné (NETRC_MACHINE2, NETRC_LOGIN2, NETRC_PASSWORD2)."
  exit 1
fi

cat > "$NETRC_FILE" <<EOF
machine ${NETRC_MACHINE1} login ${NETRC_LOGIN1} password ${NETRC_PASSWORD1}
machine ${NETRC_MACHINE2} login ${NETRC_LOGIN2} password ${NETRC_PASSWORD2}
EOF

chmod 600 "$NETRC_FILE"
echo ".netrc vytvořen s dvěma bloky podle zadaného formátu."

# Executes the passed command (e.g. your main script)
exec "$@"

