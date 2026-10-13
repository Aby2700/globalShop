
# Démonstration automatisée GlobalShop : déploiement, scénario fonctionnel,
# test de charge et test de panne.
# Usage : ./demo.sh
#         ./demo.sh --skip-chaos

set -euo pipefail
cd "$(dirname "$0")"

# Compatibilité Windows (Git Bash)
export MSYS_NO_PATHCONV=1

# Choix de Python
if command -v python3 >/dev/null 2>&1 && python3 -c "import sys" >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1 && python -c "import sys" >/dev/null 2>&1; then
  PY=python
else
  PY=py
fi

ROUTER=${ROUTER:-http://localhost:8000}
SKIP_CHAOS=0
[[ "${1:-}" == "--skip-chaos" ]] && SKIP_CHAOS=1

pp() { "$PY" -m json.tool; }
title() { echo; echo "=================== $* ==================="; }

title "1. Déploiement du cluster (4 nœuds CockroachDB + routeur)"
docker compose up -d --build

echo "Attente de la disponibilité du routeur et des 4 nœuds..."
READY=0

for i in $(seq 1 90); do
  if curl -fs "$ROUTER/health" 2>/dev/null | grep -q '"tous_up":true'; then
    READY=1
    break
  fi
  sleep 2
done

if [[ "$READY" -ne 1 ]]; then
  echo "ERREUR : le routeur ou les nœuds ne sont pas prêts."
  exit 1
fi

curl -fs "$ROUTER/health" | pp

title "2. Environnement Python des tests"

if [[ ! -d .venv ]]; then
  "$PY" -m venv .venv
fi

if [[ -f .venv/Scripts/activate ]]; then
  source .venv/Scripts/activate
else
  source .venv/bin/activate
fi

pip install -q -r tests/requirements.txt

title "3. Scénario fonctionnel"

EMAIL="awa.$RANDOM@globalshop.test"

echo "--- Inscription d'un client (Afrique)"

CLIENT=$(curl -fs -X POST "$ROUTER/clients" \
  -H 'Content-Type: application/json' \
  --data-binary "{\"nom\":\"Awa Diop\",\"email\":\"$EMAIL\",\"region\":\"afrique\"}")

echo "$CLIENT" | pp

CLIENT_ID=$(echo "$CLIENT" | "$PY" -c \
  'import sys,json; print(json.load(sys.stdin)["id"])')

echo "--- Création de produits dans 2 régions"

# Correction : envoi explicite du corps JSON
P1=$(curl -fs -X POST "$ROUTER/produits" \
  -H 'Content-Type: application/json' \
  --data-binary '{"nom":"Casque audio","prix":59.90,"stock":10,"region":"europe"}')

P2=$(curl -fs -X POST "$ROUTER/produits" \
  -H 'Content-Type: application/json' \
  --data-binary '{"nom":"Boubou brod\u00e9","prix":35.50,"stock":5,"region":"afrique"}')

echo "$P1" | pp
echo "$P2" | pp

P1_ID=$(echo "$P1" | "$PY" -c \
  'import sys,json; print(json.load(sys.stdin)["id"])')

P2_ID=$(echo "$P2" | "$PY" -c \
  'import sys,json; print(json.load(sys.stdin)["id"])')

echo "--- Catalogue distribué (lecture depuis la région du client)"
curl -fs "$ROUTER/produits?depuis=afrique&limit=5" | pp

echo "--- Commande transactionnelle : 2 produits dans 2 régions"

curl -fs -X POST "$ROUTER/commandes" \
  -H 'Content-Type: application/json' \
  --data-binary "{
    \"client_id\":\"$CLIENT_ID\",
    \"client_region\":\"afrique\",
    \"items\":[
      {\"produit_id\":\"$P1_ID\",\"produit_region\":\"europe\",\"quantite\":2},
      {\"produit_id\":\"$P2_ID\",\"produit_region\":\"afrique\",\"quantite\":1}
    ]
  }" | pp



echo "--- Tentative de survente : elle doit être refusée (409)"

HTTP_CODE=$(curl --silent --show-error \
  --output NUL \
  --write-out "%{http_code}" \
  --request POST "$ROUTER/commandes" \
  --header 'Content-Type: application/json' \
  --data-binary "{
    \"client_id\":\"$CLIENT_ID\",
    \"client_region\":\"afrique\",
    \"items\":[
      {\"produit_id\":\"$P2_ID\",\"produit_region\":\"afrique\",\"quantite\":99}
    ]
  }") || {
    echo "ERREUR : échec de la requête HTTP."
    exit 1
  }

echo "HTTP $HTTP_CODE"

if [[ "$HTTP_CODE" != "409" ]]; then
  echo "ÉCHEC : HTTP 409 était attendu."
  exit 1
else
  echo "OK : la survente est refusée."
fi


echo "--- État du cluster"
curl -fs "$ROUTER/cluster/noeuds" | pp


title "4. Test de charge : 100 achats concurrents sur un stock de 30"

"$PY" tests/load_test.py \
  --router "$ROUTER" \
  --requests 100 \
  --stock 30 || echo ">>> Le test de charge a signalé un échec (voir ci-dessus)."

if [[ "$SKIP_CHAOS" -eq 0 ]]; then
  title "5. Test de panne : arrêt d'un nœud pendant les écritures"

  "$PY" tests/chaos_test.py \
    --router "$ROUTER" \
    --victim globalshop-node-europe || echo ">>> Le test de panne a signalé un échec (voir ci-dessus)."
fi

title "Terminé"

echo "Vérifie les résultats dans le dossier ./results/."
echo "Console CockroachDB : http://localhost:8080"
