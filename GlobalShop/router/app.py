"""
GlobalShop - routeur / coordinateur de requêtes.

Rôle :
  * oriente chaque requête vers le nœud de la région du client (passerelle SQL la plus proche) ;
  * bascule automatiquement sur un autre nœud si la passerelle est injoignable (failover) ;
  * rejoue les transactions en conflit (SQLSTATE 40001) avec un backoff exponentiel ;
  * rend les écritures idempotentes (identifiants générés ici, avant la transaction), ce qui
    permet de rejouer sans risque une transaction dont l'issue est inconnue (panne pendant le COMMIT).

La cohérence forte (anti-survente) est assurée par CockroachDB (isolation SERIALIZABLE, consensus Raft)
et par la contrainte CHECK (stock >= 0) ; le routeur n'a pas besoin de verrou applicatif.
"""
import os
import random
import time
import uuid
from decimal import Decimal
from typing import List, Literal, Optional

import psycopg2
import psycopg2.errors as pgerr
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

Region = Literal["afrique", "europe", "amerique", "asie"]
REGIONS = ["afrique", "europe", "amerique", "asie"]

NODES = {}
for part in os.environ.get(
    "NODES",
    "afrique=localhost:26257,europe=localhost:26258,amerique=localhost:26259,asie=localhost:26260",
).split(","):
    k, v = part.split("=")
    NODES[k.strip()] = v.strip()

DBNAME = os.environ.get("DBNAME", "globalshop")
DBUSER = os.environ.get("DBUSER", "root")
CONNECT_TIMEOUT = int(os.environ.get("CONNECT_TIMEOUT", "3"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "25"))

app = FastAPI(title="GlobalShop router", version="1.0")


# ----------------------------------------------------------------------------
# Accès aux nœuds
# ----------------------------------------------------------------------------
class BusinessError(Exception):
    def __init__(self, status: int, detail: str):
        self.status, self.detail = status, detail


@app.exception_handler(BusinessError)
def business_error_handler(_request, exc: BusinessError):
    """Toute erreur métier remontée par une transaction devient une réponse HTTP propre."""
    return JSONResponse(status_code=exc.status, content={"detail": exc.detail})


def ordered_nodes(region: str) -> List[str]:
    """Nœud de la région d'abord (proximité), puis les autres (failover)."""
    first = [region] if region in NODES else []
    return first + [r for r in REGIONS if r != region and r in NODES]


def connect(addr: str):
    host, port = addr.rsplit(":", 1)
    return psycopg2.connect(
        host=host,
        port=int(port),
        user=DBUSER,
        dbname=DBNAME,
        sslmode="disable",
        connect_timeout=CONNECT_TIMEOUT,
        application_name="globalshop-router",
    )


def run_tx(region: str, fn):
    """
    Exécute fn(cursor) dans une transaction, avec routage + failover + retry.
    fn DOIT être idempotent (rejouable) : une panne pendant le COMMIT rend l'issue incertaine.
    Retourne (résultat, méta).
    """
    order = ordered_nodes(region)
    node_idx = 0
    serialization_retries = 0
    failovers = 0
    last_error = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        region_try = order[node_idx % len(order)]
        conn = None
        try:
            conn = connect(NODES[region_try])
            conn.autocommit = False
            cur = conn.cursor()
            result = fn(cur)
            conn.commit()
            return result, {
                "noeud_passerelle": region_try,
                "tentatives": attempt,
                "conflits_rejoues": serialization_retries,
                "bascules_noeud": failovers,
            }
        except BusinessError:
            if conn:
                _safe_rollback(conn)
            raise
        except (pgerr.SerializationFailure, pgerr.DeadlockDetected, pgerr.StatementCompletionUnknown) as e:
            # Conflit de concurrence : on rejoue sur le même nœud avec un backoff.
            last_error = e
            serialization_retries += 1
            if conn:
                _safe_rollback(conn)
            time.sleep(min(0.05 * (2 ** min(serialization_retries, 6)), 1.0) * (0.5 + random.random()))
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            # Nœud injoignable / connexion coupée : bascule sur le nœud suivant.
            last_error = e
            failovers += 1
            node_idx += 1
            if conn:
                _safe_rollback(conn)
            time.sleep(0.2)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    raise HTTPException(503, f"Service indisponible après {MAX_ATTEMPTS} tentatives : {last_error}")


def _safe_rollback(conn):
    try:
        conn.rollback()
    except Exception:
        pass


# ----------------------------------------------------------------------------
# Modèles
# ----------------------------------------------------------------------------
class ClientIn(BaseModel):
    nom: str = Field(min_length=1)
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    region: Region


class ProduitIn(BaseModel):
    nom: str = Field(min_length=1)
    prix: Decimal = Field(gt=0)
    stock: int = Field(ge=0)
    region: Region


class ItemIn(BaseModel):
    produit_id: uuid.UUID
    produit_region: Region
    quantite: int = Field(gt=0)


class CommandeIn(BaseModel):
    client_id: uuid.UUID
    client_region: Region
    items: List[ItemIn] = Field(min_length=1)
    commande_id: Optional[uuid.UUID] = None  # fourni par le client pour une idempotence de bout en bout


class ExistenceIn(BaseModel):
    ids: List[uuid.UUID]


# ----------------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------------
@app.post("/clients", status_code=201)
def inscrire_client(c: ClientIn):
    client_id = str(uuid.uuid4())  # généré avant la transaction => rejouable

    def tx(cur):
        cur.execute(
            "INSERT INTO clients (region, id, nom, email) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT DO NOTHING RETURNING id",
            (c.region, client_id, c.nom, c.email),
        )
        if cur.fetchone() is None:
            cur.execute("SELECT 1 FROM clients WHERE region = %s AND id = %s", (c.region, client_id))
            if cur.fetchone() is None:
                raise BusinessError(409, "Un client avec cet email existe déjà")
        return {"id": client_id, "region": c.region, "nom": c.nom, "email": c.email}

    res, meta = run_tx(c.region, tx)
    return {**res, "meta": meta}


@app.post("/produits", status_code=201)
def creer_produit(p: ProduitIn):
    produit_id = str(uuid.uuid4())

    def tx(cur):
        cur.execute(
            "INSERT INTO produits (region, id, nom, prix, stock) VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT DO NOTHING",
            (p.region, produit_id, p.nom, p.prix, p.stock),
        )
        return {"id": produit_id, "region": p.region, "nom": p.nom, "prix": float(p.prix), "stock": p.stock}

    res, meta = run_tx(p.region, tx)
    return {**res, "meta": meta}


@app.get("/produits")
def catalogue(
    region: Optional[Region] = None,
    depuis: Region = "europe",
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Lecture distribuée : sans filtre, le catalogue agrège les produits de toutes les régions.
    `depuis` = région du client (nœud passerelle utilisé)."""

    def tx(cur):
        if region:
            cur.execute(
                "SELECT region, id, nom, prix, stock FROM produits WHERE region = %s "
                "ORDER BY nom, id LIMIT %s OFFSET %s",
                (region, limit, offset),
            )
        else:
            cur.execute(
                "SELECT region, id, nom, prix, stock FROM produits ORDER BY nom, region, id LIMIT %s OFFSET %s",
                (limit, offset),
            )
        return [
            {"region": r[0], "id": str(r[1]), "nom": r[2], "prix": float(r[3]), "stock": r[4]}
            for r in cur.fetchall()
        ]

    res, meta = run_tx(depuis, tx)
    return {"produits": res, "meta": meta}


@app.post("/commandes", status_code=201)
def passer_commande(c: CommandeIn):
    """Transaction distribuée : décrémente le stock de chaque produit (éventuellement dans plusieurs
    régions) et crée commande + lignes + facture de façon atomique."""
    commande_id = str(c.commande_id or uuid.uuid4())

    # Fusion des doublons + ordre déterministe (réduit les conflits entre transactions concurrentes).
    merged = {}
    for it in c.items:
        key = (it.produit_region, str(it.produit_id))
        merged[key] = merged.get(key, 0) + it.quantite
    items = sorted(merged.items())

    def tx(cur):
        # Idempotence : si la commande existe déjà (rejeu après panne), on renvoie son résultat.
        cur.execute("SELECT total FROM commandes WHERE region = %s AND id = %s", (c.client_region, commande_id))
        row = cur.fetchone()
        if row:
            return {"commande_id": commande_id, "total": float(row[0]), "deja_traitee": True}

        cur.execute("SELECT 1 FROM clients WHERE region = %s AND id = %s", (c.client_region, str(c.client_id)))
        if cur.fetchone() is None:
            raise BusinessError(404, "Client inconnu")

        total = Decimal("0")
        lignes = []
        for (preg, pid), qte in items:
            # Décrément atomique conditionnel : si le stock est insuffisant, 0 ligne est modifiée.
            cur.execute(
                "UPDATE produits SET stock = stock - %s "
                "WHERE region = %s AND id = %s AND stock >= %s RETURNING prix",
                (qte, preg, pid, qte),
            )
            r = cur.fetchone()
            if r is None:
                cur.execute("SELECT stock FROM produits WHERE region = %s AND id = %s", (preg, pid))
                s = cur.fetchone()
                if s is None:
                    raise BusinessError(404, f"Produit inconnu : {preg}/{pid}")
                raise BusinessError(409, f"Stock insuffisant pour {preg}/{pid} (restant : {s[0]})")
            prix = r[0]
            total += prix * qte
            lignes.append((preg, pid, qte, prix))

        cur.execute(
            "INSERT INTO commandes (region, id, client_region, client_id, total) VALUES (%s, %s, %s, %s, %s)",
            (c.client_region, commande_id, c.client_region, str(c.client_id), total),
        )
        for preg, pid, qte, prix in lignes:
            cur.execute(
                "INSERT INTO lignes_commande (commande_region, commande_id, produit_region, produit_id, "
                "quantite, prix_unitaire) VALUES (%s, %s, %s, %s, %s, %s)",
                (c.client_region, commande_id, preg, pid, qte, prix),
            )
        cur.execute(
            "INSERT INTO factures (region, commande_id, montant) VALUES (%s, %s, %s)",
            (c.client_region, commande_id, total),
        )
        return {"commande_id": commande_id, "total": float(total), "deja_traitee": False}

    try:
        res, meta = run_tx(c.client_region, tx)
    except BusinessError as e:
        raise HTTPException(e.status, e.detail)
    except pgerr.CheckViolation:
        raise HTTPException(409, "Stock insuffisant (contrainte CHECK)")
    return {**res, "meta": meta}


@app.get("/commandes/{region}/{commande_id}")
def lire_commande(region: Region, commande_id: uuid.UUID):
    def tx(cur):
        cur.execute(
            "SELECT id, client_id, statut, total FROM commandes WHERE region = %s AND id = %s",
            (region, str(commande_id)),
        )
        r = cur.fetchone()
        if r is None:
            raise BusinessError(404, "Commande inconnue")
        return {"id": str(r[0]), "client_id": str(r[1]), "statut": r[2], "total": float(r[3])}

    try:
        res, meta = run_tx(region, tx)
    except BusinessError as e:
        raise HTTPException(e.status, e.detail)
    return {**res, "meta": meta}


@app.post("/commandes/existence")
def commandes_existantes(body: ExistenceIn):
    """Retourne, parmi les identifiants fournis, ceux qui existent en base (utilisé par les tests)."""

    def tx(cur):
        cur.execute("SELECT id FROM commandes WHERE id = ANY(%s::UUID[])", ([str(i) for i in body.ids],))
        return [str(r[0]) for r in cur.fetchall()]

    res, _ = run_tx("europe", tx)
    return {"existantes": res}


@app.get("/produits/{region}/{produit_id}/audit")
def audit_produit(region: Region, produit_id: uuid.UUID):
    """Invariant de non-survente : stock restant + quantité vendue = stock initial."""

    def tx(cur):
        cur.execute("SELECT stock FROM produits WHERE region = %s AND id = %s", (region, str(produit_id)))
        s = cur.fetchone()
        if s is None:
            raise BusinessError(404, "Produit inconnu")
        cur.execute(
            "SELECT COALESCE(SUM(quantite), 0), COUNT(*) FROM lignes_commande "
            "WHERE produit_region = %s AND produit_id = %s",
            (region, str(produit_id)),
        )
        v = cur.fetchone()
        return {"stock_restant": s[0], "quantite_vendue": int(v[0]), "nb_commandes": int(v[1])}

    try:
        res, meta = run_tx(region, tx)
    except BusinessError as e:
        raise HTTPException(e.status, e.detail)
    return {**res, "meta": meta}


@app.get("/stats")
def stats():
    def tx(cur):
        out = {}
        for t in ["clients", "produits", "commandes", "lignes_commande", "factures"]:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            out[t] = cur.fetchone()[0]
        return out

    res, meta = run_tx("europe", tx)
    return {"lignes_par_table": res, "meta": meta}


@app.get("/health")
def health():
    """État de chaque nœud vu depuis le routeur."""
    out = {}
    for region, addr in NODES.items():
        try:
            conn = connect(addr)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            out[region] = "up"
        except Exception as e:  # noqa: BLE001
            out[region] = f"down ({type(e).__name__})"
    return {"noeuds": out, "tous_up": all(v == "up" for v in out.values())}


@app.get("/cluster/noeuds")
def cluster_noeuds():
    def tx(cur):
        cur.execute("SELECT node_id, address, locality, is_live FROM crdb_internal.gossip_nodes ORDER BY node_id")
        return [
            {"node_id": r[0], "adresse": r[1], "localite": r[2], "vivant": r[3]} for r in cur.fetchall()
        ]

    res, meta = run_tx("europe", tx)
    return {"noeuds": res, "meta": meta}
