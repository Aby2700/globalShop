
CREATE DATABASE IF NOT EXISTS globalshop;
USE globalshop;

-- 3 réplicas par range : le système survit à la perte de n'importe quel nœud.
ALTER DATABASE globalshop CONFIGURE ZONE USING num_replicas = 3, gc.ttlseconds = 3600;

CREATE TYPE IF NOT EXISTS region_t AS ENUM ('afrique', 'europe', 'amerique', 'asie');

CREATE TABLE IF NOT EXISTS clients (
  region  region_t    NOT NULL,
  id      UUID        NOT NULL DEFAULT gen_random_uuid(),
  nom     TEXT        NOT NULL,
  email   TEXT        NOT NULL,
  cree_le TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (region, id)
);
CREATE UNIQUE INDEX IF NOT EXISTS clients_email_uq ON clients (email);

CREATE TABLE IF NOT EXISTS produits (
  region  region_t      NOT NULL,   -- région de l'entrepôt qui détient le stock
  id      UUID          NOT NULL DEFAULT gen_random_uuid(),
  nom     TEXT          NOT NULL,
  prix    DECIMAL(10,2) NOT NULL CHECK (prix > 0),
  stock   INT           NOT NULL CHECK (stock >= 0),   -- dernier rempart contre la survente
  cree_le TIMESTAMPTZ   NOT NULL DEFAULT now(),
  PRIMARY KEY (region, id)
);

CREATE TABLE IF NOT EXISTS commandes (
  region        region_t      NOT NULL,   -- région du client
  id            UUID          NOT NULL,
  client_region region_t      NOT NULL,
  client_id     UUID          NOT NULL,
  statut        TEXT          NOT NULL DEFAULT 'confirmee',
  total         DECIMAL(12,2) NOT NULL,
  cree_le       TIMESTAMPTZ   NOT NULL DEFAULT now(),
  PRIMARY KEY (region, id),
  FOREIGN KEY (client_region, client_id) REFERENCES clients (region, id)
);
CREATE INDEX IF NOT EXISTS commandes_client_idx ON commandes (client_region, client_id);

CREATE TABLE IF NOT EXISTS lignes_commande (
  commande_region region_t      NOT NULL,
  commande_id     UUID          NOT NULL,
  produit_region  region_t      NOT NULL,
  produit_id      UUID          NOT NULL,
  quantite        INT           NOT NULL CHECK (quantite > 0),
  prix_unitaire   DECIMAL(10,2) NOT NULL,
  PRIMARY KEY (commande_region, commande_id, produit_region, produit_id),
  FOREIGN KEY (commande_region, commande_id) REFERENCES commandes (region, id),
  FOREIGN KEY (produit_region, produit_id)   REFERENCES produits (region, id)
);
CREATE INDEX IF NOT EXISTS lignes_produit_idx ON lignes_commande (produit_region, produit_id);

CREATE TABLE IF NOT EXISTS factures (
  region      region_t      NOT NULL,
  id          UUID          NOT NULL DEFAULT gen_random_uuid(),
  commande_id UUID          NOT NULL,
  montant     DECIMAL(12,2) NOT NULL,
  emise_le    TIMESTAMPTZ   NOT NULL DEFAULT now(),
  PRIMARY KEY (region, id),
  FOREIGN KEY (region, commande_id) REFERENCES commandes (region, id)
);
CREATE UNIQUE INDEX IF NOT EXISTS factures_commande_uq ON factures (region, commande_id);
