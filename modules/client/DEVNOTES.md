# Java Client Internals

## Glossary

- Node or Server or Server Node: a single cluster member (implemented by `IgniteImpl`)
  - Multiple nodes can run in a single JVM
- Cluster: a group of interconnected nodes
- Client (former "Thin Client"): a single instance of `IgniteClient` (implemented by `TcpIgniteClient`)
  - A client connects to one or more server nodes
- Channel or Connection: a single TCP connection between a client and a server node (implemented by `TcpClientChannel`)

## Overview

## Connection Management