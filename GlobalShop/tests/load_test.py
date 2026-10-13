#!/usr/bin/env python3
"""
Test de charge anti-survente : N achats concurrents (100 par défaut) sur UN SEUL produit.

Les requêtes sont lancées exactement en même temps (barrière) depuis des clients des 4 régions,
donc via 4 nœuds passerelles différents. Invariants vérifiés :
  1. nombre d'achats réussis == stock initial (si stock < nombre de requêtes) ;
  2. stock final == 0 et jamais négatif ;
  3. stock restant + quantité vendue == stock initial (aucune survente, aucune perte) ;
  4. le reste des requêtes est refusé proprement (HTTP 409), sans erreur serveur.
"""
import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from common import REGIONS, create_clients, create_product, percentile, save_results, wait_cluster


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--router", default="http://localhost:8000")
    ap.add_argument("--requests", type=int, default=100, help="nombre d'achats concurrents")
    ap.add_argument("--stock", type=int, default=30, help="stock initial du produit")
    ap.add_argument("--product-region", default="europe", choices=REGIONS)
    args = ap.parse_args()

    wait_cluster(args.router)
    with httpx.Client(base_url=args.router, timeout=60) as c:
        produit = create_product(c, f"Dernier-article-{int(time.time())}", args.stock, args.product_region)
        clients = create_clients(c, "load")
    pid, preg = produit["id"], produit["region"]
    print(f"Produit {preg}/{pid} : stock initial = {args.stock}")
    print(f"Lancement de {args.requests} achats simultanés (clients des 4 régions)...")

    barrier = threading.Barrier(args.requests + 1)
    results = []
    lock = threading.Lock()

    def buyer(i):
        cl = clients[i % len(clients)]
        body = {
            "client_id": cl["id"],
            "client_region": cl["region"],
            "items": [{"produit_id": pid, "produit_region": preg, "quantite": 1}],
        }
        with httpx.Client(base_url=args.router, timeout=120) as http:
            barrier.wait()
            t0 = time.time()
            try:
                r = http.post("/commandes", json=body)
                status, payload = r.status_code, r.json()
            except Exception as e:  # noqa: BLE001
                status, payload = 0, {"erreur": str(e)}
            dt = time.time() - t0
        with lock:
            results.append({"status": status, "latence_s": dt, "region": cl["region"], "payload": payload})

    with ThreadPoolExecutor(max_workers=args.requests) as ex:
        futs = [ex.submit(buyer, i) for i in range(args.requests)]
        barrier.wait()
        t_start = time.time()
        for f in futs:
            f.result()
        duree = time.time() - t_start

    ok = [r for r in results if r["status"] == 201]
    refus = [r for r in results if r["status"] == 409]
    autres = [r for r in results if r["status"] not in (201, 409)]
    lat = [r["latence_s"] for r in results]
    conflits = sum(r["payload"].get("meta", {}).get("conflits_rejoues", 0) for r in ok)

    with httpx.Client(base_url=args.router, timeout=60) as c:
        audit = c.get(f"/produits/{preg}/{pid}/audit").json()

    attendu_ok = min(args.stock, args.requests)
    checks = {
        "achats_reussis == stock_initial": len(ok) == attendu_ok,
        "stock_final >= 0": audit["stock_restant"] >= 0,
        "stock_final == max(stock - requetes, 0)": audit["stock_restant"] == max(args.stock - args.requests, 0),
        "stock_restant + vendu == stock_initial": audit["stock_restant"] + audit["quantite_vendue"] == args.stock,
        "quantite_vendue == achats_reussis": audit["quantite_vendue"] == len(ok),
        "aucune erreur serveur": len(autres) == 0,
    }
    report = {
        "parametres": vars(args),
        "produit": {"id": pid, "region": preg},
        "duree_totale_s": round(duree, 3),
        "debit_req_par_s": round(args.requests / duree, 1) if duree else None,
        "achats_reussis_201": len(ok),
        "refus_rupture_409": len(refus),
        "autres_erreurs": len(autres),
        "conflits_serialisation_rejoues": conflits,
        "latence_s": {
            "p50": round(percentile(lat, 50), 3),
            "p95": round(percentile(lat, 95), 3),
            "max": round(max(lat), 3),
        },
        "reussis_par_region_client": {
            reg: sum(1 for r in ok if r["region"] == reg) for reg in REGIONS
        },
        "audit_base": audit,
        "verifications": checks,
        "succes": all(checks.values()),
    }
    path = save_results("load_test.json", report)

    print("\n=== Résultats du test de charge ===")
    print(f"  Achats réussis (201)      : {len(ok)} (attendu : {attendu_ok})")
    print(f"  Refus rupture (409)       : {len(refus)}")
    print(f"  Autres erreurs            : {len(autres)}")
    print(f"  Conflits rejoués          : {conflits}")
    print(f"  Latence p50/p95/max       : {report['latence_s']['p50']}s / {report['latence_s']['p95']}s / {report['latence_s']['max']}s")
    print(f"  Débit                     : {report['debit_req_par_s']} req/s")
    print(f"  Audit base                : {audit}")
    for k, v in checks.items():
        print(f"  [{'OK' if v else 'ÉCHEC'}] {k}")
    print(f"\nRésultats enregistrés dans {path}")
    sys.exit(0 if report["succes"] else 1)


if __name__ == "__main__":
    main()
