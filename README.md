Construction d’un Data Warehouse (Bronze → Silver → Gold)
📌 Présentation du projet

Dans un contexte moderne de Data Engineering, les entreprises doivent centraliser et structurer leurs données provenant de différentes sources (ERP, CRM, fichiers CSV, etc.) afin de faciliter l’analyse et la prise de décision.
Ce projet consiste à concevoir un Data Warehouse complet basé sur une architecture en couches : Bronze, Silver et Gold.
L’objectif principal est de transformer des données brutes en données analytiques prêtes pour le reporting et les tableaux de bord Power BI.
Ce projet simule le rôle d’un Data Engineer / Data Analyst chargé de :
concevoir un pipeline de données,
nettoyer et transformer les données,
modéliser les données pour l’analyse,
préparer les données pour le reporting.

🎯 Objectifs du projet

Les objectifs principaux du projet sont :

Construire une architecture Data Warehouse moderne
Charger des fichiers CSV dans SQL Server
Mettre en place les couches Bronze, Silver et Gold
Nettoyer et standardiser les données
Construire un modèle analytique en étoile (Star Schema)
Réaliser des contrôles de qualité des données
Créer des tableaux de bord Power BI
📂 Structure du projet

Le projet contient :

les fichiers CSV sources,
les scripts SQL pour les différentes couches,
les fichiers Power BI,
le fichier README.
🥉 Bronze Layer — Ingestion des données
🎯 Objectif

La couche Bronze contient les données brutes exactement comme elles existent dans les fichiers source, sans transformation.

Étapes réalisées
Création de la base de données DataWarehouse
Création des schémas :
bronze
silver
gold
Création des tables Bronze :
account
store
gltransaction
storemaster
account_mapping
Chargement des fichiers CSV avec BULK INSERT
Exemple SQL
BULK INSERT bronze.account
FROM 'C:\Data\account.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
🥈 Silver Layer — Nettoyage et standardisation
🎯 Objectif

La couche Silver permet de nettoyer, corriger et standardiser les données afin de les rendre exploitables pour l’analyse.

Nettoyage technique

Les opérations réalisées :

suppression des espaces inutiles avec TRIM,
uniformisation des codes avec UPPER,
conversion des types avec CAST,
gestion des valeurs NULL,
suppression des doublons.
Standardisation des données

Exemples :

correction des incohérences (P L → P&L),
harmonisation des formats texte,
normalisation des clés métiers,
uniformisation des catégories.
Règles métiers
standardisation des identifiants,
vérification de la cohérence des transactions,
validation des relations entre les tables.
🥇 Gold Layer — Modèle analytique
🎯 Objectif

Construire un modèle analytique prêt pour le reporting en utilisant un modèle en étoile (Star Schema).

Tables de dimensions
gold.dimstore

Contient les informations des magasins :

store_id
store_name
region
city
gold.dimaccount

Contient les informations des comptes financiers :

account_id
account_name
category
account_type
📌 Table de faits
gold.fact_gl

Contient les transactions enrichies :

transaction_id
account_id
store_id
amount
transaction_date
Jointures réalisées

Les principales jointures sont :

Transactions ↔ Comptes
Transactions ↔ Magasins
🧪 Contrôle qualité des données
🎯 Objectif

Garantir la qualité et l’intégrité des données à chaque étape.

Vérifications réalisées
détection des doublons,
vérification des valeurs NULL,
contrôle des clés étrangères,
validation des relations entre tables,
comparaison des volumes entre Bronze, Silver et Gold.
Exemple SQL
SELECT account_id, COUNT(*)
FROM silver.account
GROUP BY account_id
HAVING COUNT(*) > 1;
📊 Power BI Layer — Reporting & Visualisation
🎯 Objectif

Transformer les données analytiques en tableaux de bord interactifs.

Dashboards réalisés
Dashboard Financier (P&L)

Analyse :

revenus,
coûts,
profits,
évolution financière.
Dashboard Performance Magasins

Analyse :

performances par magasin,
comparaison régionale,
KPI opérationnels.
Analyse temporelle

Analyse :

évolution mensuelle,
tendances annuelles,
analyse saisonnière.
🛠️ Technologies utilisées
SQL Server
T-SQL
Power BI
CSV Files
🚀 Workflow ETL
Chargement des fichiers CSV dans Bronze
Nettoyage et standardisation dans Silver
Construction du modèle analytique dans Gold
Contrôle qualité des données
Création des dashboards Power BI
📚 Concepts abordés
Architecture Data Warehouse
ETL (Extract Transform Load)
SQL Server
Modélisation en étoile
Nettoyage des données
Contrôle qualité
Reporting Power BI
✅ Résultat attendu

À la fin du projet :

les données sont centralisées,
les données sont nettoyées et standardisées,
le modèle analytique est optimisé,
les dashboards Power BI permettent l’analyse métier,
l’architecture est scalable et maintenable.
