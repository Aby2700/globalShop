"""Utilitaires communs aux scripts de test."""
import json
import os
import time
import uuid

import httpx

REGIONS = ["afrique", "europe", "amerique", "asie"]
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(ROOT, "results")


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    k = min(len(s) - 1, max(0, int(round(p / 100 * (len(s) - 1)))))
    return s[k]


def wait_cluster(base, timeout=180):
    """Attend que le routeur réponde et que les 4 nœuds soient 'up'."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = httpx.get(f"{base}/health", timeout=10)
            last = r.json()
            if last.get("tous_up"):
                return last
        except Exception as e:  # noqa: BLE001
            last = str(e)
        time.sleep(2)
    raise SystemExit(f"Cluster non prêt après {timeout}s : {last}")


def create_product(c, nom, stock, region, prix="19.90"):
    r = c.post("/produits", json={"nom": nom, "prix": prix, "stock": stock, "region": region})
    r.raise_for_status()
    return r.json()


def create_clients(c, prefix="test"):
    out = []
    for region in REGIONS:
        r = c.post(
            "/clients",
            json={"nom": f"Client {region}", "email": f"{prefix}-{uuid.uuid4().hex[:10]}@globalshop.test", "region": region},
        )
        r.raise_for_status()
        out.append(r.json())
    return out


def save_results(name, data):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    path = os.path.join(RESULTS_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return path
