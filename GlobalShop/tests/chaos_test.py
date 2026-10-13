#!/usr/bin/env python3
"""
Chaos engineering : on tue brutalement un nœud (docker kill = SIGKILL) PENDANT une charge d'écritures,
puis on le redémarre. Chronologie par défaut : charge 0-45 s, kill à 8 s, redémarrage à 28 s.

Les clients rejouent les commandes dont l'issue est incertaine (même identifiant => idempotent).
Vérifications finales :
  1. aucune commande confirmée (HTTP 201) n'a été perdue ;
  2. stock restant + quantité vendue == stock initial (cohérence conservée) ;
  3. les commandes d'issue inconnue sont soit entièrement appliquées, soit pas du tout (atomicité) ;
  4. le nœud tué rejoint le cluster après redémarrage.
"""
import argparse
import subprocess
import sys
import threading
import time
import uuid

import httpx

from common import REGIONS, create_clients, create_product, percentile, save_results, wait_cluster


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--router", default="http://localhost:8000")
    ap.add_argument("--victim", default="globalshop-node-europe", help="nom du conteneur à tuer")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--duration", type=float, default=45)
    ap.add_argument("--kill-at", type=float, default=8)
    ap.add_argument("--restart-at", type=float, default=28)
    ap.add_argument("--stock", type=int, default=100000)
    ap.add_argument("--product-region", default="amerique", choices=REGIONS)
    ap.add_argument("--client-retries", type=int, default=6)
    ap.add_argument("--request-timeout", type=float, default=30)
    ap.add_argument("--no-kill", action="store_true", help="témoin : ne tue aucun nœud")
    args = ap.parse_args()

    wait_cluster(args.router)
    with httpx.Client(base_url=args.router, timeout=60) as c:
        produit = create_product(c, f"Chaos-{int(time.time())}", args.stock, args.product_region)
        clients = create_clients(c, "chaos")
    pid, preg = produit["id"], produit["region"]
    print(f"Produit {preg}/{pid}, stock initial {args.stock}, victime : {args.victim}")

    stop = threading.Event()
    records = []
    lock = threading.Lock()
    T0 = time.time()

    def worker(wid):
        cl = clients[wid % len(clients)]
        with httpx.Client(base_url=args.router, timeout=args.request_timeout) as http:
            while not stop.is_set():
                oid = str(uuid.uuid4())
                body = {
                    "client_id": cl["id"],
                    "client_region": cl["region"],
                    "commande_id": oid,
                    "items": [{"produit_id": pid, "produit_region": preg, "quantite": 1}],
                }
                t0 = time.time()
                outcome, tries = "inconnu", 0
                for _ in range(args.client_retries):
                    tries += 1
                    try:
                        r = http.post("/commandes", json=body)
                        if r.status_code == 201:
                            outcome = "ok"
                            break
                        if r.status_code in (404, 409):
                            outcome = "refuse"
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(0.5)
                t1 = time.time()
                with lock:
                    records.append(
                        {"id": oid, "outcome": outcome, "tries": tries, "t_start": t0 - T0, "t_end": t1 - T0,
                         "latence_s": t1 - t0, "region": cl["region"]}
                    )

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(args.workers)]
    for t in threads:
        t.start()

    events = []

    def log_event(name, extra=""):
        t = time.time() - T0
        events.append({"t": round(t, 2), "evenement": name, "detail": extra})
        print(f"[t={t:5.1f}s] {name} {extra}")

    killed = restarted = False
    while time.time() - T0 < args.duration:
        now = time.time() - T0
        if not args.no_kill and not killed and now >= args.kill_at:
            p = subprocess.run(["docker", "kill", args.victim], capture_output=True, text=True)
            log_event("KILL", f"{args.victim} rc={p.returncode} {p.stderr.strip()}")
            killed = True
        if not args.no_kill and killed and not restarted and now >= args.restart_at:
            p = subprocess.run(["docker", "start", args.victim], capture_output=True, text=True)
            log_event("RESTART", f"{args.victim} rc={p.returncode} {p.stderr.strip()}")
            restarted = True
        time.sleep(0.2)
    stop.set()
    for t in threads:
        t.join(timeout=args.request_timeout * args.client_retries)
    log_event("FIN_CHARGE")

    # Attente du retour complet du cluster
    try:
        etat = wait_cluster(args.router, timeout=180)
        log_event("CLUSTER_COMPLET", str(etat["noeuds"]))
    except SystemExit as e:
        log_event("CLUSTER_INCOMPLET", str(e))

    # ---- Vérifications ----
    ok_ids = [r["id"] for r in records if r["outcome"] == "ok"]
    unk_ids = [r["id"] for r in records if r["outcome"] == "inconnu"]
    ref_ids = [r["id"] for r in records if r["outcome"] == "refuse"]

    def existing(ids):
        found = set()
        with httpx.Client(base_url=args.router, timeout=120) as c:
            for i in range(0, len(ids), 500):
                r = c.post("/commandes/existence", json={"ids": ids[i:i + 500]})
                r.raise_for_status()
                found |= set(r.json()["existantes"])
        return found

    found_ok = existing(ok_ids)
    found_unk = existing(unk_ids)
    with httpx.Client(base_url=args.router, timeout=60) as c:
        audit = c.get(f"/produits/{preg}/{pid}/audit").json()
        noeuds = c.get("/cluster/noeuds").json()["noeuds"]

    perdues = [i for i in ok_ids if i not in found_ok]
    vendu_attendu = len(found_ok) + len(found_unk)

    # Disponibilité : plus long intervalle entre deux succès consécutifs
    ends = sorted(r["t_end"] for r in records if r["outcome"] == "ok")
    gaps = [(b - a, a) for a, b in zip(ends, ends[1:])]
    plus_long_trou, trou_debut = max(gaps) if gaps else (None, None)
    echecs_tentatives = sum(r["tries"] - 1 for r in records)
    lat = [r["latence_s"] for r in records if r["outcome"] == "ok"]

    # Débit par tranche de 5 s
    buckets = {}
    for r in records:
        if r["outcome"] == "ok":
            b = int(r["t_end"] // 5) * 5
            buckets[b] = buckets.get(b, 0) + 1
    debit_par_5s = [{"debut_s": b, "commandes_ok": n} for b, n in sorted(buckets.items())]

    checks = {
        "aucune commande confirmee perdue": len(perdues) == 0,
        "stock_restant + vendu == stock_initial": audit["stock_restant"] + audit["quantite_vendue"] == args.stock,
        "quantite_vendue == commandes presentes en base": audit["quantite_vendue"] == vendu_attendu,
        "stock jamais negatif": audit["stock_restant"] >= 0,
    }
    if not args.no_kill:
        checks["noeud tue de retour dans le cluster"] = sum(1 for n in noeuds if n["vivant"]) == len(REGIONS)

    report = {
        "parametres": vars(args),
        "evenements": events,
        "commandes_confirmees": len(ok_ids),
        "commandes_refusees": len(ref_ids),
        "commandes_issue_incertaine": len(unk_ids),
        "dont_appliquees_en_base": len(found_unk),
        "commandes_confirmees_perdues": len(perdues),
        "tentatives_rejouees_total": echecs_tentatives,
        "plus_long_intervalle_sans_succes_s": round(plus_long_trou, 2) if plus_long_trou is not None else None,
        "intervalle_commence_a_t_s": round(trou_debut, 2) if trou_debut is not None else None,
        "latence_succes_s": {"p50": round(percentile(lat, 50), 3), "p95": round(percentile(lat, 95), 3),
                             "max": round(max(lat), 3)} if lat else None,
        "debit_par_tranche_5s": debit_par_5s,
        "audit_base": audit,
        "noeuds_finaux": noeuds,
        "verifications": checks,
        "succes": all(checks.values()),
    }
    path = save_results("chaos_test.json" if not args.no_kill else "chaos_temoin.json", report)

    print("\n=== Résultats du test de panne ===")
    print(f"  Commandes confirmées      : {len(ok_ids)} (perdues : {len(perdues)})")
    print(f"  Issue incertaine          : {len(unk_ids)} (appliquées en base : {len(found_unk)})")
    print(f"  Tentatives rejouées       : {echecs_tentatives}")
    print(f"  Plus long trou sans succès: {report['plus_long_intervalle_sans_succes_s']} s")
    print(f"  Audit base                : {audit}")
    for k, v in checks.items():
        print(f"  [{'OK' if v else 'ÉCHEC'}] {k}")
    print(f"\nRésultats enregistrés dans {path}")
    sys.exit(0 if report["succes"] else 1)


if __name__ == "__main__":
    main()
