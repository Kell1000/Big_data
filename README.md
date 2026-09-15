# Hadoop léger sous Docker — PC 8 Go RAM / 128 Go SSD

Architecture retenue : **pseudo-distribué** (1 seul nœud logique, plusieurs conteneurs légers), avec montée en charge **par phases**. On ne lance jamais tout l'écosystème en même temps sur 8 Go.

**Règle d'or 8 Go** : Windows + WSL2 + Docker Desktop consomment déjà ~2-3 Go à vide. Il reste ~5 Go pour les services. On ne fait jamais tourner Hive + Spark + Kafka simultanément avec Hadoop tant que le besoin réel ne l'impose pas.

---

## STEP 0 — Prérequis (vérifications système)

**Concept** : avant d'installer quoi que ce soit, on vérifie que la machine peut supporter la virtualisation (WSL2 = base de Docker Desktop sous Windows).

**Explication** : WSL2 tourne dans une VM légère Hyper-V ; il faut la virtualisation matérielle activée dans le BIOS, et suffisamment de RAM/disque libres.

**Commandes** :

| # | Commande | Où l'exécuter | Ce que ça fait |
|---|----------|----------------|-----------------|
| 1 | `winver` | Exécuter (Win+R) | Affiche la version de Windows (il faut Windows 10 2004+ ou Windows 11) |
| 2 | `systeminfo | findstr /C:"Total Physical Memory"` | PowerShell | RAM totale installée |
| 3 | `Get-PSDrive C` | PowerShell | Espace disque libre sur le SSD |
| 4 | `systeminfo | findstr /C:"Hyper-V"` | PowerShell (admin) | Vérifie si la virtualisation est activée |

Pour activer la virtualisation si désactivée : redémarrer, entrer dans le BIOS (F2/Del/F10 selon la marque), activer **Intel VT-x** ou **AMD-V**.

✅ **Validation avant de continuer** : Windows 10/11 à jour, ≥ 20 Go libres sur le SSD, virtualisation activée.

---

## STEP 1 — Installer WSL2 + Ubuntu

**Concept** : WSL2 est requis par Docker Desktop pour exécuter les conteneurs Linux avec de bonnes performances sur Windows.

**Commandes** (PowerShell en administrateur) :

```powershell
wsl --install
wsl --set-default-version 2
wsl --install -d Ubuntu-22.04
```

- `wsl --install` : installe le sous-système WSL et son noyau Linux.
- `wsl --set-default-version 2` : force WSL2 (plus rapide que WSL1) comme version par défaut.
- `wsl --install -d Ubuntu-22.04` : installe une distribution Ubuntu légère à l'intérieur de WSL2.

Après redémarrage, Ubuntu se lance et demande un nom d'utilisateur/mot de passe Linux (à créer, indépendant de Windows).

Vérification (PowerShell) :
```powershell
wsl -l -v
```
Doit afficher `Ubuntu-22.04` avec `VERSION 2`.

✅ **Validation** : `wsl -l -v` montre Ubuntu en version 2.

---

## STEP 2 — Installer Docker Desktop

**Concept** : Docker Desktop fournit le moteur Docker sur Windows, avec un backend WSL2 (plus léger qu'un backend Hyper-V classique).

**Étapes** :
1. Télécharger Docker Desktop depuis le site officiel.
2. Lancer l'installeur, cocher **"Use WSL 2 instead of Hyper-V"**.
3. Redémarrer si demandé.
4. Lancer Docker Desktop, aller dans **Settings → General**, vérifier que *"Use the WSL 2 based engine"* est coché.
5. Dans **Settings → Resources → WSL Integration**, activer l'intégration avec `Ubuntu-22.04`.

Vérification (Ubuntu/WSL) :
```bash
docker --version
docker compose version
```

✅ **Validation** : les deux commandes renvoient un numéro de version sans erreur.

---

## STEP 3 — Configurer Docker pour 8 Go de RAM

**Concept** : par défaut Docker Desktop peut s'auto-allouer trop de RAM. Il faut la plafonner pour laisser de la marge à Windows.

Dans **Docker Desktop → Settings → Resources → Advanced** :

| Paramètre | Valeur recommandée | Pourquoi |
|---|---|---|
| CPU | 2-3 cœurs | Laisse le reste à Windows |
| Memory | **4 Go max** | Laisse ~4 Go à Windows + navigateur, etc. |
| Swap | 1 Go | Coussin en cas de pic |
| Disk image size | 30-40 Go | Suffisant pour Phase 1 à 3, à surveiller |
| Disk image location | Garder sur le SSD principal, éviter un disque externe lent | Performance |

Alternative si vous préférez configurer via fichier (WSL2 backend), créer `%UserProfile%\.wslconfig` :
```ini
[wsl2]
memory=4GB
processors=2
swap=1GB
```
Puis dans PowerShell : `wsl --shutdown` (redémarre WSL avec la nouvelle config).

Vérification :
```bash
docker info | grep -i "Total Memory"
```

✅ **Validation** : Docker limité à 4 Go, Windows garde le reste.

---

## PART 2 — Fondamentaux Docker (le minimum vital)

| Concept | Explication courte |
|---|---|
| **Image** | Modèle en lecture seule (ex: `bde2020/hadoop-namenode`) à partir duquel on crée des conteneurs. |
| **Conteneur** | Instance en cours d'exécution d'une image, isolée mais légère (pas une VM complète). |
| **Volume** | Espace disque persistant en dehors du conteneur (survit à `docker compose down`). |
| **Réseau** | Réseau virtuel Docker permettant aux conteneurs de se parler par leur nom (ex: `namenode:9000`). |
| **Dockerfile** | Recette pour construire une image personnalisée. |
| **Docker Compose** | Fichier YAML décrivant plusieurs conteneurs liés (notre cas : namenode, datanode, resourcemanager, nodemanager). |
| **Port mapping** | `9870:9870` = expose le port du conteneur sur le port de Windows pour accéder aux interfaces web. |
| **Variables d'environnement** | Paramètres passés au conteneur au démarrage (ex: config Hadoop). |
| **Logs** | `docker logs <conteneur>` — indispensable pour diagnostiquer. |
| **Cycle de vie** | `create → start → stop → restart → rm`. |

**Exemple pratique** :
```bash
docker run --rm hello-world      # image + conteneur éphémère
docker network create test-net   # réseau
docker volume create test-vol    # volume
```

**Pourquoi Docker pour apprendre Hadoop ?** Installer Hadoop nativement sous Windows est pénible (variables d'environnement, `winutils.exe`, versions Java). Docker encapsule tout ça dans des images prêtes à l'emploi, reproductibles, et jetables (on peut tout supprimer et recommencer en 2 minutes).

---

## PART 3 — Architecture retenue

Sur 8 Go, un cluster multi-nœuds complet est exclu. On choisit un **pseudo-cluster à 4 conteneurs légers** (1 NameNode, 1 DataNode, 1 ResourceManager, 1 NodeManager), chacun limité en RAM via Compose.

```
Windows 11
│
├── WSL2 (Ubuntu 22.04)
│
└── Docker Desktop
    │
    ├── [namenode]         → HDFS métadonnées, UI :9870
    ├── [datanode]         → HDFS stockage réel des blocs
    ├── [resourcemanager]  → YARN, ordonnancement des jobs, UI :8088
    └── [nodemanager]      → YARN, exécute les tâches sur le nœud
```

| Rôle | Explication |
|---|---|
| **NameNode** | Le "cerveau" HDFS : sait où sont les fichiers/blocs, mais ne stocke pas les données elles-mêmes. |
| **DataNode** | Stocke physiquement les blocs de données. |
| **ResourceManager** | Alloue les ressources (CPU/RAM) aux applications YARN. |
| **NodeManager** | Exécute les conteneurs de calcul sur la machine locale, rapporte au ResourceManager. |

**Pourquoi 4 conteneurs et pas 1 seul ?** Séparer les rôles reproduit fidèlement l'architecture réelle de Hadoop (pédagogiquement utile) tout en restant très léger (chaque conteneur ~256-512 Mo).

---

## PART 4 — Structure du projet

```
hadoop-docker/
├── docker-compose.yml     # orchestration des 4 conteneurs
├── .env                   # variables (versions, ports, mémoire)
├── config/
│   └── hadoop.env         # configuration Hadoop partagée
├── data/                  # données locales à uploader dans HDFS
└── README.md
```

- `config/hadoop.env` : centralise les paramètres HDFS/YARN (remplace des XML complexes grâce aux images bde2020, qui les génèrent automatiquement).
- `data/` : vos fichiers texte (ex: `etudiants.csv`) avant de les charger dans HDFS.

Créer la structure (Ubuntu/WSL) :
```bash
mkdir -p ~/hadoop-docker/config ~/hadoop-docker/data
cd ~/hadoop-docker
```

---

## PART 5 — docker-compose.yml (Phase 1 : HDFS + YARN + MapReduce)

On utilise les images **bde2020/hadoop** (Hadoop 3.2.1), légères et très utilisées en pédagogie.

`.env` :
```env
HADOOP_VERSION=3.2.1
CORE_CONF_fs_defaultFS=hdfs://namenode:8020
```

`config/hadoop.env` :
```env
CORE_CONF_fs_defaultFS=hdfs://namenode:8020
CORE_CONF_hadoop_http_staticuser_user=root
HDFS_CONF_dfs_replication=1
YARN_CONF_yarn_resourcemanager_hostname=resourcemanager
YARN_CONF_yarn_nodemanager_aux___services=mapreduce_shuffle
YARN_CONF_yarn_log___aggregation___enable=true
MAPRED_CONF_mapreduce_framework_name=yarn
MAPRED_CONF_yarn_app_mapreduce_am_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/
MAPRED_CONF_mapreduce_map_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/
MAPRED_CONF_mapreduce_reduce_env=HADOOP_MAPRED_HOME=/opt/hadoop-3.2.1/
```

`docker-compose.yml` :
```yaml
version: "3.8"

networks:
  hadoop-net:
    driver: bridge

volumes:
  namenode-data:
  datanode-data:

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    restart: unless-stopped
    environment:
      - CLUSTER_NAME=lab-hadoop
    env_file: ./config/hadoop.env
    ports:
      - "9870:9870"   # UI NameNode
      - "8020:8020"
    volumes:
      - namenode-data:/hadoop/dfs/name
    networks: [hadoop-net]
    mem_limit: 512m
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9870"]
      interval: 30s
      retries: 5

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    restart: unless-stopped
    env_file: ./config/hadoop.env
    environment:
      - SERVICE_PRECONDITION=namenode:9870
    ports:
      - "9864:9864"   # UI DataNode
    volumes:
      - datanode-data:/hadoop/dfs/data
    networks: [hadoop-net]
    depends_on: [namenode]
    mem_limit: 512m

  resourcemanager:
    image: bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8
    container_name: resourcemanager
    restart: unless-stopped
    env_file: ./config/hadoop.env
    environment:
      - SERVICE_PRECONDITION=namenode:9870 datanode:9864
    ports:
      - "8088:8088"   # UI ResourceManager
    networks: [hadoop-net]
    depends_on: [namenode, datanode]
    mem_limit: 512m

  nodemanager:
    image: bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
    container_name: nodemanager
    restart: unless-stopped
    env_file: ./config/hadoop.env
    environment:
      - SERVICE_PRECONDITION=namenode:9870 datanode:9864 resourcemanager:8088
    ports:
      - "8042:8042"   # UI NodeManager
    networks: [hadoop-net]
    depends_on: [resourcemanager]
    mem_limit: 512m
```

**Points clés expliqués** :
- `mem_limit: 512m` sur chaque service → total ≈ 2 Go max pour Phase 1, laisse de la marge sur les 4 Go alloués à Docker.
- `depends_on` + `SERVICE_PRECONDITION` : évite que le DataNode démarre avant que le NameNode soit prêt.
- `restart: unless-stopped` uniquement sur les services de base — pas de `always`, pour pouvoir tout arrêter proprement.
- Volumes nommés → les données HDFS survivent à un `docker compose down` (mais pas à `down -v`).

---

## STEP 6 — Démarrer Hadoop

```bash
cd ~/hadoop-docker
docker compose up -d          # démarre les 4 conteneurs en arrière-plan
docker compose ps             # vérifie l'état (Up/healthy)
docker compose logs -f namenode   # suit les logs du NameNode (Ctrl+C pour quitter)
```

Vérifications dans le navigateur Windows :
- NameNode : http://localhost:9870
- ResourceManager : http://localhost:8088
- DataNode : http://localhost:9864
- NodeManager : http://localhost:8042

Pour tout arrêter proprement :
```bash
docker compose down          # arrête et supprime les conteneurs (garde les volumes)
docker compose down -v       # + supprime les volumes (repart de zéro)
```

✅ **Validation** : les 4 UIs répondent, le NameNode UI (9870) affiche "1 Live Node" dans l'onglet Datanodes.

---

## STEP 7 — Pratique HDFS

**Concept** : HDFS = système de fichiers distribué. On interagit avec via `hdfs dfs`.

```bash
# entrer dans le conteneur namenode
docker exec -it namenode bash

# état général
hdfs dfsadmin -report

# créer une arborescence (exemple contextualisé Maroc/OFPPT)
hdfs dfs -mkdir -p /data/etudiants
hdfs dfs -mkdir -p /data/logs

# lister
hdfs dfs -ls /data

# uploader un fichier local
echo "id,nom,ville,note
1,Yassine,Casablanca,15
2,Salma,Rabat,17
3,Omar,Marrakech,12" > /tmp/etudiants.csv
hdfs dfs -put /tmp/etudiants.csv /data/etudiants/

# lire
hdfs dfs -cat /data/etudiants/etudiants.csv

# télécharger
hdfs dfs -get /data/etudiants/etudiants.csv /tmp/telecharge.csv

# supprimer
hdfs dfs -rm /data/etudiants/etudiants.csv

# blocs et réplication
hdfs fsck /data -files -blocks
hdfs dfs -stat %r /data/etudiants/etudiants.csv

# usage disque
hdfs dfs -du -h /data
```

✅ **Validation** : le fichier apparaît dans l'UI NameNode (Utilities → Browse the file system).

---

## STEP 9 — MapReduce (WordCount)

**Concept** : `Input (HDFS) → Mapper (clé/valeur) → Shuffle/Sort → Reducer → Output (HDFS)`.

```bash
# données d'entrée (dans le conteneur namenode)
echo "big data hadoop spark hadoop big data casablanca hadoop" > /tmp/texte.txt
hdfs dfs -mkdir -p /data/input
hdfs dfs -put /tmp/texte.txt /data/input/

# lancer le job WordCount fourni avec Hadoop
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar \
  wordcount /data/input /data/output

# lire le résultat
hdfs dfs -cat /data/output/part-r-00000
```

Suivi du job : http://localhost:8088 (onglet Applications) — vous verrez l'Application ID, son statut, ses logs.

**Concept algorithme distribué** : le Mapper transforme chaque mot en paire `(mot, 1)` en parallèle sur des blocs de données ; le Shuffle regroupe toutes les paires ayant la même clé sur le même Reducer ; le Reducer additionne les occurrences. C'est le principe même de la parallélisation "diviser pour régner" que vous voyez en cours d'Algorithmes Distribués.

✅ **Validation** : `part-r-00000` contient bien le décompte des mots (`hadoop 3`, `big 2`, `data 2`, etc.)

---

## PART 9 (YARN en pratique)

```
Client → ResourceManager → ApplicationMaster → NodeManager → Conteneurs
```

- UI ResourceManager (:8088) : liste des applications, leur statut (RUNNING/FINISHED/FAILED).
- Cliquer sur une application → voir l'ApplicationMaster et ses logs.
- En ligne de commande :
```bash
yarn application -list
yarn application -status <application_id>
yarn logs -applicationId <application_id>
```

---

## STEP 10 — Ajouter Hive (Phase 2, seulement après validation Phase 1)

⚠️ **Avant de démarrer Hive : réduire/arrêter temporairement d'autres apps Windows gourmandes**. Ajoutez au `docker-compose.yml` un service `hive-server` (image `bde2020/hive:2.3.2-postgresql-metastore`) + `hive-metastore-postgresql`. Comptez **+1,5 à 2 Go de RAM**.

```
HDFS → Hive (métadonnées SQL) → requêtes SQL
```

```sql
CREATE DATABASE ofppt_lab;
USE ofppt_lab;

CREATE TABLE etudiants (
  id INT, nom STRING, ville STRING, note INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH '/data/etudiants/etudiants.csv' INTO TABLE etudiants;

SELECT ville, AVG(note) AS moyenne
FROM etudiants
GROUP BY ville;
```

Hive stocke les données dans HDFS (`/user/hive/warehouse`) et les métadonnées dans une base Postgres dédiée (le metastore). **Ne le faites pas tourner en permanence sur 8 Go** : démarrez-le seulement pour la session de travail (`docker compose up -d hive-server hive-metastore-postgresql`), puis `docker compose stop hive-server hive-metastore-postgresql` après usage.

---

## STEP 11 — Ajouter Spark (Phase 3)

Image légère : `bitnami/spark` en mode local (`local[*]`), pas de cluster Spark séparé — cela évite d'ajouter des Workers gourmands.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LabCasablanca") \
    .master("local[2]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

df = spark.read.csv("hdfs://namenode:8020/data/etudiants/etudiants.csv", header=True, inferSchema=True)
df.groupBy("ville").avg("note").show()
```

- **RDD** : collection distribuée immuable de bas niveau.
- **DataFrame** : RDD + schéma, optimisé (Catalyst).
- **Spark vs MapReduce** : Spark garde les données en mémoire entre étapes (beaucoup plus rapide pour les jobs itératifs), MapReduce écrit sur disque à chaque étape.

Limiter la mémoire du driver Spark : `--driver-memory 1g --executor-memory 1g`.

---

## STEP 12 — Ajouter Kafka (Phase 4, dernière étape)

Pour 8 Go, choisissez **Kafka en mode KRaft** (sans Zookeeper) — un conteneur en moins, moins de RAM (image `bitnami/kafka` avec KRaft activé).

```
Producer → Topic "etudiants" → Consumer
```

```bash
# créer le topic
kafka-topics.sh --create --topic etudiants --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# producteur (terminal 1)
kafka-console-producer.sh --topic etudiants --bootstrap-server localhost:9092

# consommateur (terminal 2)
kafka-console-consumer.sh --topic etudiants --bootstrap-server localhost:9092 --from-beginning
```

---

## PART 13 — Architecture finale complète

```
Docker
│
├── Hadoop (permanent en phase d'apprentissage active)
│   ├── NameNode / DataNode
│   └── ResourceManager / NodeManager
│
├── Hive        (démarré ponctuellement, dialogue avec HDFS)
├── Spark       (démarré ponctuellement, lit/écrit sur HDFS)
└── Kafka       (indépendant de HDFS, pour le streaming)
```

---

## PART 14 — Gestion des ressources (table 8 Go)

| Composant | RAM recommandée | Tourner en permanence ? |
|---|---:|---|
| NameNode | 512 Mo | Oui (Phase 1+) |
| DataNode | 512 Mo | Oui (Phase 1+) |
| ResourceManager | 512 Mo | Oui (Phase 1+) |
| NodeManager | 512 Mo | Oui (Phase 1+) |
| Hive (server+metastore) | ~1,5-2 Go | **Non** — à la demande seulement |
| Spark (local mode) | 1-1,5 Go | **Non** — à la demande seulement |
| Kafka (KRaft) | ~700 Mo-1 Go | **Non** — à la demande seulement |

**RAM Docker max recommandée** : 4 Go (sur les 8 Go totaux).

**Ne jamais lancer simultanément** : Hive + Spark + Kafka en plus de Hadoop → dépasserait les 4 Go alloués. Combinez au maximum deux composants "à la demande" à la fois (ex: Hadoop + Spark, puis on arrête Spark avant de lancer Hive).

**Éviter de saturer le SSD (128 Go)** :
```bash
docker system df                 # voir l'usage disque Docker
docker image prune -a            # supprimer les images inutilisées
docker container prune           # supprimer les conteneurs arrêtés
docker volume prune              # ⚠️ supprime les volumes non utilisés (perte de données HDFS si non attachés)
docker system prune -a --volumes # nettoyage complet (à utiliser avec précaution)
```

---

## PART 15 — Dépannage (Symptôme → Cause → Diagnostic → Solution)

**Docker daemon ne démarre pas**
→ Cause : WSL2 non lancé ou Docker Desktop pas complètement démarré.
→ Diagnostic : `wsl -l -v`, ouvrir Docker Desktop et regarder l'icône.
→ Solution : `wsl --shutdown` puis relancer Docker Desktop.

**Erreur "virtualisation désactivée"**
→ Cause : VT-x/AMD-V désactivé dans le BIOS.
→ Diagnostic : `systeminfo | findstr Hyper-V`.
→ Solution : activer dans le BIOS.

**"Not enough memory" au démarrage des conteneurs**
→ Cause : RAM Docker limitée dépassée (trop de services actifs).
→ Diagnostic : `docker stats`.
→ Solution : arrêter Hive/Spark/Kafka, ne garder que Hadoop.

**NameNode ne démarre pas**
→ Cause : volume corrompu ou reformatage nécessaire.
→ Diagnostic : `docker compose logs namenode`.
→ Solution : `docker compose down -v` puis `docker compose up -d` (recrée un HDFS propre).

**DataNode ne se connecte pas**
→ Cause : NameNode pas encore prêt, ou IDs de cluster désynchronisés.
→ Diagnostic : `docker compose logs datanode`.
→ Solution : redémarrer l'ordre `namenode` puis `datanode` ; en dernier recours `down -v`.

**HDFS en Safe Mode**
→ Cause : démarrage en cours ou trop peu de blocs répliqués.
→ Diagnostic : `hdfs dfsadmin -safemode get`.
→ Solution : `hdfs dfsadmin -safemode leave` (uniquement si le cluster est stable).

**Erreurs de permission HDFS**
→ Cause : utilisateur différent de `root`/propriétaire du dossier HDFS.
→ Diagnostic : `hdfs dfs -ls -d /data`.
→ Solution : `hdfs dfs -chmod -R 777 /data` (labo uniquement, jamais en prod).

**ResourceManager indisponible**
→ Cause : démarré avant le NameNode/DataNode.
→ Diagnostic : `docker compose logs resourcemanager`.
→ Solution : vérifier `SERVICE_PRECONDITION`, redémarrer dans l'ordre.

**Port déjà utilisé (ex: 8088, 9870)**
→ Cause : un autre service Windows/Docker occupe le port.
→ Diagnostic : `netstat -ano | findstr 8088` (PowerShell).
→ Solution : changer le mapping dans `docker-compose.yml` (ex: `"8089:8088"`).

**Conteneurs qui ne se joignent pas**
→ Cause : pas sur le même réseau Docker.
→ Diagnostic : `docker network inspect hadoop-net`.
→ Solution : vérifier que tous les services listent `networks: [hadoop-net]`.

**Docker consomme trop de disque**
→ Diagnostic : `docker system df`.
→ Solution : `docker image prune -a`, vérifier la taille du disque virtuel dans Docker Desktop Settings.

---

## PART 16 — Checklist de validation

```text
[ ] Docker installé et fonctionnel
[ ] WSL2 opérationnel (Ubuntu 22.04, version 2)
[ ] Conteneurs Hadoop up (namenode, datanode, resourcemanager, nodemanager)
[ ] NameNode UI accessible (:9870)
[ ] DataNode connecté (visible dans l'UI NameNode)
[ ] HDFS : création/upload/lecture/suppression de fichiers OK
[ ] YARN : ResourceManager UI accessible (:8088)
[ ] MapReduce : WordCount exécuté avec succès
[ ] Hive : requêtes SQL exécutées sur données HDFS
[ ] Spark : DataFrame lu depuis HDFS
[ ] Kafka : producteur/consommateur fonctionnels
```

Séquence finale de vérification :
```bash
docker compose ps
hdfs dfsadmin -report
yarn application -list
hdfs dfs -ls /data
```

---

## PART 17 — 10 labs progressifs

| # | Lab | Objectif | Dataset | Résultat attendu |
|---|---|---|---|---|
| 1 | Fondamentaux Docker | Comprendre image/conteneur/volume/réseau | — | Conteneur `hello-world` lancé |
| 2 | Bases HDFS | Créer une arborescence HDFS | `etudiants.csv` | Dossiers visibles dans l'UI |
| 3 | Gestion de fichiers HDFS | Upload/download/suppression | `etudiants.csv` | Cycle complet réussi |
| 4 | MapReduce WordCount | Comprendre Map/Shuffle/Reduce | `texte.txt` | Comptage correct des mots |
| 5 | Supervision YARN | Suivre un job dans l'UI | Job du lab 4 | Statut FINISHED visible |
| 6 | SQL avec Hive | Requêtes SQL sur HDFS | `etudiants.csv` | `GROUP BY ville` fonctionnel |
| 7 | Spark DataFrame | Manipuler des données en mémoire | `etudiants.csv` | Agrégations correctes |
| 8 | Spark + HDFS | Lire/écrire directement sur HDFS | `etudiants.csv` | Fichier de sortie dans HDFS |
| 9 | Kafka producteur/consommateur | Comprendre le streaming | Messages texte | Message reçu par le consommateur |
| 10 | Mini-projet Big Data | Intégrer HDFS→Spark→sortie | Données étudiants OFPPT (Casablanca, Rabat, Marrakech...) | Rapport de moyennes par ville |

Pour chaque lab : partez toujours de l'objectif → concepts du cours "Algorithmes Distribués" (CAP, Paxos/Raft pour comprendre *pourquoi* HDFS réplique, DAG pour comprendre l'ordonnancement Spark) → commande → résultat observé dans l'UI ou le terminal.

---

### Suite recommandée
Je vous propose de valider d'abord la **Phase 1 (STEP 0 à 9)** de bout en bout avant d'ajouter Hive. Dites-moi quand HDFS/YARN/MapReduce tournent correctement, et je détaille le `docker-compose.yml` complet avec Hive puis Spark puis Kafka intégrés, service par service.
