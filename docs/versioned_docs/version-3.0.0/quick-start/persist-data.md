---
title: Getting Started with Ignite 3 Persistent Storage
sidebar_label: Persist Data
---

{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}

## Introduction

### About this Guide

This guide will walk you through the basics of setting up and using Ignite 3's RocksDB-based persistent storage with the Chinook database in a Docker-based environment.

### Prerequisites

- Up-to-date versions of Docker and Docker Compose;
- Terminal or command line access;
- Basic SQL knowledge;
- At least 8GB of free RAM for the Ignite cluster.

## Understanding Persistence in Ignite 3

### Persistence Architecture

Ignite persistence is designed to provide quick and responsive persistent storage. When using persistent storage:

- Ignite stores all its data on disk;
- It loads as much data as possible into RAM for processing;
- Data is split into multiple partitions, with each partition stored in a separate file on disk;
- In addition to data partitions, Ignite stores indexes and metadata on disk.

This architecture combines the performance benefits of in-memory computing with the durability of disk-based storage.

### Storage Engine Types

#### Persistent Storage Options

- **AIPersist Engine** - Default persistent storage engine with checkpointing;
- **RocksDB Engine** - LSM-tree based persistent storage optimized for write-heavy workloads.

#### Volatile Storage Options

- **AIMem Engine** - In-memory storage with no persistence.

### Storage Profiles

In Ignite 3, persistence is configured by using **storage profiles**. A storage profile defines how data is stored, cached, and managed by the storage engine.

Each storage profile has specific properties depending on the engine type, but all profiles must specify the following properties:

- **name** - A unique identifier for the profile
- **engine** - The storage engine to use

### Distribution Zones

Distribution zones control how data is distributed across the cluster and which storage profiles to use. They allow you to:

- Control the number of data replicas;
- Specify which nodes can store data;
- Define how data is partitioned;
- Assign storage profiles to determine persistence type.

## Setting Up a Persistent Cluster

### Docker Environment Configuration

We will use Docker Compose to create a multi-node Ignite cluster with persistent storage.

### Creating the Docker Compose File

Create a `docker-compose.yml` file in your working directory:

```yaml
name: ignite3

x-ignite-def: &ignite-def
  image: apacheignite/ignite:3.1.0
  environment:
    JVM_MAX_MEM: "4g"
    JVM_MIN_MEM: "4g"
  configs:
    - source: node_config
      target: /opt/ignite/etc/ignite-config.conf

services:
  node1:
    <<: *ignite-def
    command: --node-name node1
    ports:
      - "10300:10300"
      - "10800:10800"
    volumes:
      - ./data/node1:/opt/ignite/work

  node2:
    <<: *ignite-def
    command: --node-name node2
    ports:
      - "10301:10300"
      - "10801:10800"
    volumes:
      - ./data/node2:/opt/ignite/work

  node3:
    <<: *ignite-def
    command: --node-name node3
    ports:
      - "10302:10300"
      - "10802:10800"
    volumes:
      - ./data/node3:/opt/ignite/work

configs:
  node_config:
    content: |
      ignite {
        network {
          port: 3344
          nodeFinder.netClusterNodes = ["node1:3344", "node2:3344", "node3:3344"]
        }
        "storage": {
          "profiles": [
            {
              name: "rocksDbProfile"
              engine: "rocksdb"
            }
          ]
        }
      }
```

The `node_config` configuration in the Docker Compose file:

- Adds a storage profile named `rocksDbProfile` that uses the RocksDB engine.
- Sets the storage size to 256MB (268435456 bytes) by default.
- Stores persistent data in the `data` directory where Docker was run.

### Starting the Cluster

Run the following command to start your cluster:

```bash
docker-compose up -d
```

### Verifying Cluster Deployment

Check that all nodes are running:

```shell
docker compose ps
```

You should see output similar to:

```
NAME              IMAGE                       COMMAND                  SERVICE   CREATED          STATUS          PORTS
ignite3-node1-1   apacheignite/ignite:3.0.0   "docker-entrypoint.s…"   node1     37 seconds ago   Up 33 seconds   0.0.0.0:10300->10300/tcp, 3344/tcp, 0.0.0.0:10800->10800/tcp
ignite3-node2-1   apacheignite/ignite:3.0.0   "docker-entrypoint.s…"   node2     37 seconds ago   Up 33 seconds   3344/tcp, 0.0.0.0:10301->10300/tcp, 0.0.0.0:10801->10800/tcp
ignite3-node3-1   apacheignite/ignite:3.0.0   "docker-entrypoint.s…"   node3     37 seconds ago   Up 33 seconds   3344/tcp, 0.0.0.0:10302->10300/tcp, 0.0.0.0:10802->10800/tcp
```

Verify the Docker network:

```shell
docker network ls
```

## Configuring Persistent Storage

### Connecting to the Cluster

Connect to the Ignite CLI:

```bash
docker run --rm -it --network=host -e LANG=C.UTF-8 -e LC_ALL=C.UTF-8 apacheignite/ignite:3.0.0 cli
```

When the CLI tool offers to connect to default node, confirm the connection. If you ever get disconnected, you can connect again by typing the following command:

```bash
connect http://localhost:10300
```

### Initializing the Cluster

Before using the cluster, initialize it:

```shell
cluster init --name=ignite3 --metastorage-group=node1,node2,node3
```

You should see the message "Cluster was initialized successfully".

### Examining Storage Profiles

Verify the configured storage profiles:

```shell
node config show ignite.storage
```

You should see output showing the `rocksDbProfile` configuration along with the default profiles.

### Creating Distribution Zones for Persistence

Enter the interactive SQL CLI:

```shell
sql
```

Create a distribution zone that uses our RocksDB storage profile:

```sql
CREATE ZONE ChinookRocksDB WITH replicas=2, storage_profiles='rocksDbProfile';
```

## Building the Chinook Database with Persistence

### About the Chinook Database

The Chinook database represents a digital media store with tables for artists, albums, tracks, and more. It's commonly used as a sample database for demonstrating database features.

### Creating Persistent Database Tables

Create the necessary tables for the Chinook database using our RocksDB persistent zone:

```sql
-- Create Artist table
CREATE TABLE Artist (
    ArtistId INT NOT NULL,
    Name VARCHAR(120),
    PRIMARY KEY (ArtistId)
) ZONE ChinookRocksDB;

-- Create Album table
CREATE TABLE Album (
    AlbumId INT NOT NULL,
    Title VARCHAR(160) NOT NULL,
    ArtistId INT NOT NULL,
    PRIMARY KEY (AlbumId, ArtistId)
) COLOCATE BY (ArtistId) ZONE ChinookRocksDB;

-- Create Genre table
CREATE TABLE Genre (
    GenreId INT NOT NULL,
    Name VARCHAR(120),
    PRIMARY KEY (GenreId)
) ZONE ChinookRocksDB;

-- Create MediaType table
CREATE TABLE MediaType (
    MediaTypeId INT NOT NULL,
    Name VARCHAR(120),
    PRIMARY KEY (MediaTypeId)
) ZONE ChinookRocksDB;

-- Create Track table
CREATE TABLE Track (
    TrackId INT NOT NULL,
    Name VARCHAR(200) NOT NULL,
    AlbumId INT,
    MediaTypeId INT NOT NULL,
    GenreId INT,
    Composer VARCHAR(220),
    Milliseconds INT NOT NULL,
    Bytes INT,
    UnitPrice NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (TrackId, AlbumId)
) COLOCATE BY (AlbumId) ZONE ChinookRocksDB;
```

### Loading Sample Data

Insert sample data into the tables:

```sql
-- Insert data into MediaType table
INSERT INTO MediaType (MediaTypeId, Name) VALUES
(1, 'MPEG audio file'),
(2, 'Protected AAC audio file');

-- Insert data into Artist table
INSERT INTO Artist (ArtistId, Name) VALUES
(1, 'AC/DC'),
(2, 'Accept'),
(3, 'Aerosmith'),
(4, 'Alanis Morissette'),
(5, 'Alice In Chains');

-- Insert data into Album table
INSERT INTO Album (AlbumId, Title, ArtistId) VALUES
(1, 'For Those About To Rock We Salute You', 1),
(2, 'Balls to the Wall', 2),
(3, 'Restless and Wild', 2),
(4, 'Let There Be Rock', 1),
(5, 'Big Ones', 3);

-- Insert data into Genre table
INSERT INTO Genre (GenreId, Name) VALUES
(1, 'Rock'),
(2, 'Jazz'),
(3, 'Metal'),
(4, 'Alternative & Punk'),
(5, 'Rock And Roll');

-- Insert data into Track table
INSERT INTO Track (TrackId, Name, AlbumId, MediaTypeId, GenreId, Composer, Milliseconds, Bytes, UnitPrice) VALUES
(1, 'For Those About To Rock (We Salute You)', 1, 1, 1, 'Angus Young, Malcolm Young, Brian Johnson', 343719, 11170334, 0.99),
(2, 'Balls to the Wall', 2, 2, 1, 'U. Dirkschneider, W. Hoffmann, H. Frank, P. Baltes, S. Kaufmann, G. Hoffmann', 342562, 5510424, 0.99),
(3, 'Fast As a Shark', 3, 2, 1, 'F. Baltes, S. Kaufman, U. Dirkschneider & W. Hoffman', 230619, 3990994, 0.99),
(4, 'Restless and Wild', 3, 2, 1, 'F. Baltes, R.A. Smith-Diesel, S. Kaufman, U. Dirkschneider & W. Hoffman', 252051, 4331779, 0.99),
(5, 'Princess of the Dawn', 3, 2, 1, 'Deaffy & R.A. Smith-Diesel', 375418, 6290521, 0.99);
```

### Querying the Database

Test that your data was inserted correctly:

```sql
SELECT a.Name AS Artist, al.Title AS Album, t.Name AS Track
FROM Track t
JOIN Album al ON t.AlbumId = al.AlbumId
JOIN Artist a ON al.ArtistId = a.ArtistId
WHERE t.AlbumId = 1;
```

## Testing Persistence Capabilities

### Verifying Data Before Restart

Perform additional queries to ensure your data is properly stored:

```sql
-- Count tracks by genre
SELECT g.Name AS Genre, COUNT(t.TrackId) AS TrackCount
FROM Track t
JOIN Genre g ON t.GenreId = g.GenreId
GROUP BY g.Name;

-- Check all albums by artist
SELECT a.Name AS Artist, COUNT(al.AlbumId) AS AlbumCount
FROM Album al
JOIN Artist a ON al.ArtistId = a.ArtistId
GROUP BY a.Name;
```

### Restarting the Cluster

To restart the cluster, you need to first exit the CLI tool.

- Exit the SQL CLI with the `exit;` command,
- Then exit the main CLI with the `exit` command.

Restart the Docker containers:

```bash
docker-compose down
docker-compose up -d
```

### Verifying Data Persistence After Restart

Reconnect to the CLI:

```bash
docker run --rm -it --network=host -e LANG=C.UTF-8 -e LC_ALL=C.UTF-8 apacheignite/ignite:3.0.0 cli
```

The cluster is already initialized, so you can go directly to the SQL CLI:

```shell
sql
```

Run the same query to verify the data persisted through the restart:

```sql
SELECT a.Name AS Artist, al.Title AS Album, t.Name AS Track
FROM Track t
JOIN Album al ON t.AlbumId = al.AlbumId
JOIN Artist a ON al.ArtistId = a.ArtistId
WHERE t.AlbumId = 1;
```

## Wrap Up

### Summary

Ignite 3 with RocksDB persistent storage provides a powerful way to maintain data durability while leveraging in-memory computing performance. RocksDB is particularly well-suited for write-intensive workloads, making it an excellent choice for many production environments.

### Additional Resources

- [RocksDB Documentation](https://rocksdb.org/docs/)
- [Chinook Database Project](https://github.com/lerocha/chinook-database)
