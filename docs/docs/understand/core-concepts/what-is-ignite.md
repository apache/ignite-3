---
id: what-is-ignite
title: What is Apache Ignite 3?
sidebar_position: 1
---

# What is Apache Ignite 3?

Apache Ignite 3 is the next step of development of Apache Ignite, reworking the product from its core to bring it to a new era of interconnected networks and fast-paced and fast scaling environments. This page helps you to quickly see the improvements over Ignite 2.

## Core Improvements

Ignite core features have been reworked to better align with the most common usages.

### Streamlined Tables

In Ignite 2, data was stored in "Caches" in Binary Object format. To avoid discrepancies in SQL and other APIs, Ignite 3 was changed to directly work with both SQL and Key-Value API. So, data is now stored in "Tables".

Due to the way data was stored in Ignite 2, the same storage was represented by multiple objects with separate schemas. This could lead to issues in configuring the relationships and potential loss of data consistency. In Ignite 3, all objects are mapped to the same schema. This way you only have one point of interaction and a universally recognized API, regardless of what you store and what your goals are.

### Expanded and Ubiquitous Transactions

Support for transactions in Ignite 2 required changing the atomicity of the cache and worked at the cost of cache performance. Transaction configuration was equally tricky, requiring the correct concurrency and isolation configuration to achieve the best results. Transactions support in Ignite 3 is vastly improved:

- All tables are transactional by default, with no major effect on performance
- Both SQL and key-value transactions are supported
- All transactions are MVCC based, ensuring the lack of deadlocks
- All transactions are serializable
- New read-only transactions are introduced, not requiring a lock on data

### Simpler Data Collocation and Distribution

Ignite 2 had multiple ways to handle data distribution in the cluster (data affinity, backup configuration, baseline topology, etc). The configuration you need may be difficult to find, and it was hard to understand how configurations were interacting with each other. Ignite 3 combines all properties related to data distribution into the distribution zones. In distribution zone configuration, you set up how you want to store data and where. Data collocation is also tied to distribution zones, all you need to specify is the key you want to collocate data by.

Ignite 3 removes custom affinity functions and instead uses the rendezvous hashing algorithm to reliably deliver consistent data distribution. Unlike in Ignite 2, you cannot create custom affinity function. This was a powerful functionality, but slight issues with custom implementation could cause security and stability risks. Instead, most cases that previously required custom affinity can now be handled safely and securely by [distribution zones](/docs/3.1.0/sql/reference/language-definition/distribution-zones).

## Better Clients

Ignite 3 dispenses with the concept of thick clients, instead providing thin clients for all interactions with the cluster. With Ignite 3 clients, your applications do not need to get the whole Ignite application to handle operations.

The option to start Ignite from code as a client node remains, but this mode of operation is no longer the primary way to work with the cluster. This is now called running Ignite in Embedded mode.

## Finer Control over Distributed Computing

Ignite 3 distributed computing API extends what was possible in Ignite 2. All features of Ignite 2 distributed computing are kept, and new APIs were added to keep active track of your job execution queues and job failover.

## Management Improvements

Ignite 2 management tools provide access to all of its features, but in a number of areas it was difficult to get to specific parts of configuration, or it could be less transparent in where to configure the specific property.

Ignite 3 configuration is provided in lightweight and human-readable HOCON format, which is easy to both understand and edit. Configuration is also split between cluster-wide and individual node configuration, making each of them easier to manage.

### Management Tools

Over its lifetime, Ignite 2 collected a number of management scripts, each having a separate purpose. This could lead to confusion when using the scripts to configure your cluster, or having to use multiple different scripts to configure something specific. The scripts were also part of the distribution.

Ignite 3 provides a brand-new management tool that can communicate with any node in the cluster to seamlessly apply the configuration, and provides you with an option to work in interactive mode with command autocomplete. This new tool is also provided as a separate distribution, so that it can be set up on any machine that has access to the node address.
