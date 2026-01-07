# Client Internals

## Glossary

- **Node** or **Server** or **Server Node**: a single cluster member (see `IgniteImpl`)
  - Multiple nodes can run in a single JVM
- **Cluster**: a group of interconnected **nodes**
- **Client** (former "Thin Client"): a single instance of `IgniteClient` (see `TcpIgniteClient`)
  - A **client** connects to one or more **servers**
- **Channel** or **Connection**: a single TCP connection between a **client** and a **server** (see `TcpClientChannel`)

## Overview

## Connection Management