-- Pré-découpage explicite des tables par région (fragmentation horizontale visible dès le départ).
-- Sans cela, CockroachDB garde une petite table dans un seul range (découpage automatique à ~512 Mio).
-- Chaque range résultant est répliqué 3 fois et son leaseholder est placé par l'allocateur du cluster.
USE globalshop;

ALTER TABLE clients         SPLIT AT VALUES ('afrique'::region_t), ('europe'::region_t), ('amerique'::region_t), ('asie'::region_t);
ALTER TABLE produits        SPLIT AT VALUES ('afrique'::region_t), ('europe'::region_t), ('amerique'::region_t), ('asie'::region_t);
ALTER TABLE commandes       SPLIT AT VALUES ('afrique'::region_t), ('europe'::region_t), ('amerique'::region_t), ('asie'::region_t);
ALTER TABLE lignes_commande SPLIT AT VALUES ('afrique'::region_t), ('europe'::region_t), ('amerique'::region_t), ('asie'::region_t);
ALTER TABLE factures        SPLIT AT VALUES ('afrique'::region_t), ('europe'::region_t), ('amerique'::region_t), ('asie'::region_t);
