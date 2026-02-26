---
title: Code Deployment
sidebar_label: Code Deployment
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

When working with Ignite 3, you may need to deploy user code to cluster nodes so that it can be executed across the cluster, as shown in the [Distributed Computing](../compute/compute.md) section.

In Ignite 3 the code is deployed as an immutable **deployment unit** with a unique ID and version.

While there are no strict policies on what a deployment unit can contain, Ignite 3 currently supports compute jobs implemented in Java and .NET.

:::note
You can invoke compute job execution from any client (.NET, Java, C++, etc. ), but the job itself must be written in Java or .NET.
:::

If you want to use any other programming language in a compute job, you must load that file as part of the job's code. A code file deployed on its own will not be loaded by the JVM and thus cannot be used directly.

The example below demonstrates how to load a script that is packaged within a JAR's resources:

```java
public class MyJob implements ComputeJob<String, String> {
    @Override
    public CompletableFuture<String> executeAsync(JobExecutionContext ctx, String arg) {
        Ignite ignite = ctx.ignite();

        /** Full path to the script we want to run */
        final String resPath = "/org/apache/ignite/example/code/deployment/resources/script.sh";

        try (InputStream in = MyJob.class.getResourceAsStream(resPath)) {
            if (in == null) {
                throw new IllegalStateException("Resource not found: " + resPath);
            }

            byte[] script = in.readAllBytes();

            Process p = new ProcessBuilder("sh", "-s", "--", arg)
                    .redirectErrorStream(true)
                    .start();

            try (OutputStream os = p.getOutputStream()) {
                os.write(script);
            }

            String out;
            try (InputStream procOut = p.getInputStream()) {
                out = new String(procOut.readAllBytes(), StandardCharsets.UTF_8).strip();
            }

            int exit = p.waitFor();
            if (exit != 0) {
                throw new RuntimeException("Script exited with code " + exit + ":\n" + out);
            }

            String result = "Node: " + ignite.name()
                    + "\nArg: " + arg
                    + "\nScript output:\n" + out;

            return CompletableFuture.completedFuture(result);
        } catch (Exception e) {
            throw new RuntimeException("Failed to run script", e);
        }
    }
}
```

You can manage deployment units using either [CLI](../../ignite-cli-tool.md) commands or the [REST API](https://ignite.apache.org/releases/3.0.0/openapi.yaml). Both methods provide the same functionality for deploying, listing, and undeploying code.

## Deploying Units with Folder Structures

:::note
Currently, you can only deploy ZIP archives via REST.
:::

Apache Ignite supports deploying units that contain folder structures using ZIP archives. You can package complex deployment units with multiple files organized in directories, which are automatically extracted and preserved during deployment.

To deploy your code this way, package your files into a ZIP archive and deploy it to the cluster. Apache Ignite will preserve the folder structure.

## Deployment Unit Location

By default, nodes store the deployment units in the `{IGNITE_HOME}/work/deployment` directory. This can be changed with the [`ignite.deployment.location`](../../administrators-guide/config/node-config.md#code-deployment-configuration) node configuration parameter.

The deployment units have the following structure:

```
deployment
├─ unit1Id
│  ├─ version1
│  └─ version2
└─ unit2Id
   ├─ version1
   └─ version2
```

Each deployment unit is stored in a separate directory, with each version having its own subdirectory.

## Deploy New Unit

Deploying a new unit requires specifying a unique string ID for the code and a version number.

:::note
To update the code, deploy a new unit. The new unit can use the same ID as the existing one, but it must have a different version.
:::


### Deploy via CLI

When deploying a new unit, use `cluster unit deploy` command with unit's ID and set the following options:

| Parameter | Description |
|-----------|-------------|
| version | **Required** Deployment unit version in `x.y.z` format. If a unit with the same name and version already exists, a `Unit already exists` error will be thrown. |
| path | **Required** Path to the deployment unit file or directory. It is recommended to use an absolute path. |
| nodes | Defines the target nodes for deployment. Use `ALL` to deploy to every node immediately, `MAJORITY` to deploy to enough nodes for a management group majority (with remaining nodes updated later), or list specific node names (comma-separated) for immediate deployment to those nodes. |

:::note
You cannot deploy multiple units simultaneously, you need to run `unit deploy` command separately for each file you want to deploy.
:::

For example, to deploy to the majority of nodes use the following command:

```bash
cluster unit deploy test-unit --version 1.0.0 --path $ABSOLUTE_PATH_TO_CODE_UNIT --nodes MAJORITY
```

Here `$ABSOLUTE_PATH_TO_CODE_UNIT` refers to the absolute path to the code unit file or directory.

### Deploy via REST

To deploy a new unit via the REST API, send a `POST` request to the `/management/v1/deployment/units/{unitId}/{unitVersion}` endpoint with the following parameters:

| Parameter | Type | Description |
|-----------|------|-------------|
| unitId | path | **Required** Unique unit ID. If a deployment unit with this ID does not exist, it is created. |
| unitVersion | path | **Required** Unique version of the deployment unit. If a deployment unit with the specified ID and version already exists, HTTP 409 "Conflict" response is returned. |
| unitContent | file (multipart) | **Required** JAR file to deploy, provided as a file upload via multipart/form-data. |
| deployMode | query | Defines how many nodes the unit will be deployed to. If set to `MAJORITY`, the unit will be deployed to enough nodes to form cluster management group majority. If set to `ALL`, the unit will be deployed to all nodes. Cannot be used with the `initialNodes` parameter. |
| initialNodes | query | The list of names of specific nodes to deploy the unit to. Cannot be used with the `deployMode` parameter. |

For example, you can deploy a new unit to specific nodes in your local cluster as follows:

```bash
curl -X POST 'http://localhost:10300/management/v1/deployment/units/unit/1.0.0?initialNodes=node1,node2' \
  -H "Content-Type: multipart/form-data" \
  -F "unitContent=@/path/to/your/unit.jar"
```

- You can target nodes using either the `deployMode` or `initialNodes` parameter. These options serve the same purpose as the similar CLI parameters, ensuring the unit propagates as needed.

- For additional details see the corresponding [API documentation](https://ignite.apache.org/releases/3.0.0/openapi.yaml).

### Deploy Manually

If necessary, you can deploy a new unit manually by adding your code to the deployment unit storage on the node. Unlike other deployment options, node restart is required to load new deployment units.

To deploy the code:

- Find the [deployment unit location](#deployment-unit-location) on the node.
- Create a new directory. This directory will be used as the deployment unit ID.
- Create a new subdirectory. This directory will be used as the deployment unit version. You must use [semantic version](https://semver.org/) as its name.
- Add your code to the subdirectory.
- Restart the node to load the new code.

As a result, your directory structure may look like this:

```
deployment
└─ myUnit
  └─ 1.0.0
     └─ [code files]
```

## Getting Unit Information

This section explains how get all deployments on the cluster or on a specific node, view unit details such as status and version, and search or filter deployments by these attributes.

### Get Unit Information via CLI

You can list deployment units using `unit list` command.

:::note
When you run the `unit list` command in the CLI, the output shows a list of deployment units. An asterisk (*) indicates the active version, which is always the highest [semantic version](https://semver.org/), regardless of deployment order.
:::

- Use `cluster unit list` command to see all deployed units on the cluster.

- Use `node unit list` command to view only the units on the node where the command is executed.

- Pass the unit's ID to the command to get information for the specific unit:

```bash
cluster unit list test-unit
```

- Search units by adding `version` command options:

```bash
cluster unit list test-unit --version 1.0.0
```

- Or filter by `status`:

```bash
cluster unit list test-unit --status deployed
```

| Parameter | Description |
|-----------|-------------|
| statuses | Filter units by status.<br/><br/>- `UPLOADING` - the unit is being deployed to the cluster<br/>- `DEPLOYED` - the unit is deployed to the cluster and can be used<br/>- `OBSOLETE` - the command to remove unit has been received, but it is still used in some jobs<br/>- `REMOVING` - the unit is being removed<br/><br/>If not specified, deployment units in all statuses will be returned. |


### Get Unit Information via REST

You can also retrieve deployment unit details via `GET` requests.

- To get information for a specific unit on a node or across the cluster, use `/management/v1/deployment/node/units/{unitId}` and `/management/v1/deployment/cluster/units/{unitId}` respectively.

```bash
curl -X GET 'http://localhost:10300/management/v1/deployment/cluster/units/test-unit/1.0.0'
```

- To list all deployment units for the node or across the cluster, use `/management/v1/deployment/node/units` and `/management/v1/deployment/cluster/units` respectively.

```bash
curl -X GET 'http://localhost:10300/management/v1/deployment/cluster/units/'
```

- You can further narrow down the search by looking up only deployments with specific versions or statuses.

| Parameter | Type | Description |
|-----------|------|-------------|
| unitId | path | **Required** Unique unit ID of the deployment unit. |
| version | query | Unique version of the deployment unit. If not specified, all versions of deployment unit will be returned. |
| statuses | query | Statuses of the deployment units to return. Possible values:<br/><br/>- `UPLOADING` - the unit is being deployed to the cluster<br/>- `DEPLOYED` - the unit is deployed to the cluster and can be used<br/>- `OBSOLETE` - the command to remove unit has been received, but it is still used in some jobs<br/>- `REMOVING` - the unit is being removed<br/><br/>If not specified, deployment units in all statuses will be returned. |


## Undeploying Unit

When you no longer need a deployment unit version, you can undeploy it from the cluster.

### Undeploy via CLI

Use the `cluster unit undeploy` command. Provide unit ID and unit `version` to remove.

```bash
cluster unit undeploy test-unit --version 1.0.0
```

- You cannot undeploy all units with the same ID at once, you must remove them by version.

- When you undeploy a unit that has multiple versions, the active code rolls back to the next most recent version, determined by the version number.


### Undeploy via REST

To undeploy a unit from specific nodes, use a `DELETE` request to `/management/v1/deployment/units/{unitId}/{unitVersion}` endpoint.

For instance, to undeploy the same unit from nodes node1 and node2, use the following command:

```bash
curl -X DELETE 'http://localhost:10300/management/v1/deployment/units/test-unit/1.0.0?nodes=node1,node2'
```

When the cluster receives the request, it will delete the specified deployment unit version on all nodes.
If the unit is used in a job, it will instead be moved to the `OBSOLETE` status and removed once it is no longer required.
