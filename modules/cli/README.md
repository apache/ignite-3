# Ignite CLI module

Ignite CLI tool is a single entry point for any management operations.

## Build

    mvn package -f ../../pom.xml
## Run
For Windows:

    target/ignite.exe
For Linux/MacOS:

    ./target/ignite
## Examples
Download and prepare artifacts for running an Ignite node:

    ignite init
Node start:

    ignite start consistent-id
Node stop:

    ignite stop consistent-id

Cluster initialization:

    ignite cluster init --cluster-name=cluster-name --node-endpoint=localhost:10300 --meta-storage-node=consistent-id --cmg-node=consistent-id

Get current node configuration:

    ignite config get --node-endpoint=localhost:10300
Show help:

    ignite --help
    ignite init --help
    ...
