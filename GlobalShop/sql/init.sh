#!/bin/bash
# Initialisation du cluster CockroachDB puis application du schéma.
CRDB=/cockroach/cockroach
HOST=node-afrique:26257

echo "[init] initialisation du cluster..."
for i in $(seq 1 60); do
  out=$($CRDB init --insecure --host=$HOST 2>&1) && { echo "[init] cluster initialisé"; break; }
  if echo "$out" | grep -qi "already been initialized"; then
    echo "[init] cluster déjà initialisé"; break
  fi
  sleep 2
done

echo "[init] attente de disponibilité SQL..."
for i in $(seq 1 60); do
  $CRDB sql --insecure --host=$HOST -e "SELECT 1" >/dev/null 2>&1 && break
  sleep 2
done

echo "[init] application du schéma..."
$CRDB sql --insecure --host=$HOST -f /schema.sql || exit 1

# Découpage par région : étape non bloquante (un échec ne doit pas empêcher le démarrage du système).
echo "[init] découpage des tables par région..."
$CRDB sql --insecure --host=$HOST -f /splits.sql || echo "[init] AVERTISSEMENT : découpage manuel non appliqué (sans gravité)"
echo "[init] terminé"
