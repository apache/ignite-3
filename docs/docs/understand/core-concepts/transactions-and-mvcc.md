---
id: transactions-and-mvcc
title: Transactions and MVCC
sidebar_position: 3
---

# Transactions and MVCC

Ignite 3 uses MVCC (Multi-Version Concurrency Control) for transaction isolation. This provides several benefits:

- All tables are transactional by default
- All transactions are serializable
- Deadlock-free operation
- Support for read-only transactions

For details on using transactions, see the [Transactions documentation](/docs/3.1.0/developers-guide/transactions).
