---
id: install-kubernetes
title: Install on Kubernetes
sidebar_label: Kubernetes
---

You can install Apache Ignite 3 and run an Apache Ignite cluster on Kubernetes cluster. This section describes all the necessary steps, as well as provides the configurations and manifests that you can copy and paste into your environment.

:::note
Using a Helm chart is recommended for production deployments, however, if you choose not to use Helm, this guide will walk you through installing Apache Ignite on Kubernetes manually.
:::

## Prerequisites

### Recommended Kubernetes Version

Apache Ignite 3 requires Kubernetes 1.20 or later.

## Installation Steps

### Create ConfigMaps

1. Create the Apache Ignite configuration file. The minimum node configuration is as follows:

```json title="ignite-config.conf"
ignite: {
  network: {
    # Apache Ignite 3 node port
    port = 3344
    nodeFinder = {
      netClusterNodes = [
        # Kubernetes service to access the Apache Ignite 3 cluster on the Kubernetes network
        "ignite-svc-headless:3344"
      ]
    }
  }

  storage: {
    profiles = [
      {
        engine = "aipersist"
        name = "default"
        replacementMode = "CLOCK"
        # Explicit storage size configuration
        sizeBytes = 2147483648
      }
    ]
  }
}
```

2. Create the ConfigMap object for Apache Ignite configuration:

```shell
kubectl create configmap ignite-config -n <namespace> --from-file=ignite-config.conf
```

Replace `<namespace>` with the name of the namespace where you want to deploy Apache Ignite.

:::note
In Kubernetes deployments, the `ignite-config.conf` file is mounted as a read-only ConfigMap, so any attempt to update it with the `node config update` command will fail.

To update Apache Ignite node configuration, modify the existing ConfigMap and restart all Apache Ignite pods.
:::

- Modify previously configured ConfigMap object:

```bash
kubectl edit configmap ignite-config -n <namespace>
```

- Restart Apache Ignite pod, repeat for every pod:

```bash
kubectl delete pod <Apache Ignite pod name> -n <namespace>
```

### Create and Deploy the Service

Depending on your requirements, define and deploy a Kubernetes service. Apache Ignite 3 use two types of services: one for internal cluster discovery, and the other for external client access.

1. First, choose a type of service you need and prepare the `service.yaml` file.

- For communication inside the Kubernetes cluster, Use a headless service by setting the `clusterIP` parameter to `None`. This will expose each pod's IP, enabling Apache Ignite to be partition-aware: clients discover every node's address, determine which partition resides on which node, and send requests directly where the data is located.

```yaml title="service.yaml"
apiVersion: v1
kind: Service
metadata:
  # The name must be equal to netClusterNodes.
  name: ignite-svc-headless
  # Place your namespace name here.
  namespace: <namespace>
spec:
  clusterIP: None
  internalTrafficPolicy: Cluster
  ipFamilies:
  - IPv4
  ipFamilyPolicy: SingleStack
  ports:
  - name: management
    port: 10300
    protocol: TCP
    targetPort: 10300
  - name: rest
    port: 10800
    protocol: TCP
    targetPort: 10800
  - name: cluster
    port: 3344
    protocol: TCP
    targetPort: 3344
  selector:
    # Must be equal to the label set for pods.
    app: ignite
  # Include not-yet-ready nodes.
  publishNotReadyAddresses: True
  sessionAffinity: None
  type: ClusterIP
```

- Use a `LoadBalancer` service to allow external clients to connect. Keep in mind, that with this option you giving up partition awareness.

If your environments does not support `LoadBalancer`, you can use `type: NodePort` instead. Refer to the Kubernetes [documentation](https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/) for details.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ignite-loadbalancer
  labels:
    app: ignite
spec:
  type: LoadBalancer
  selector:
    app: ignite
  ports:
    - name: rest
      protocol: TCP
      port: 10800
      targetPort: 10800
    - name: client
      port: 10300
      protocol: TCP
      targetPort: 10300
```

2. Then apply the `service.yaml` file to set up this service:

```shell
kubectl apply -f service.yaml
```

### Deploy the StatefulSet

1. Prepare the `statefulset.yaml` file for StatefulSet deployment:

```yaml title="statefulset.yaml"
apiVersion: apps/v1
kind: StatefulSet
metadata:
  # The cluster name.
  name: ignite-cluster
  # Place your namespace name.
  namespace: <namespace>
spec:
  # The initial number of pods to be started by Kubernetes.
  replicas: 2
  # Kubernetes service to access the Ignite 3 cluster on the Kubernetes network.
  serviceName: ignite-svc-headless
  selector:
    matchLabels:
      app: ignite
  template:
    metadata:
      labels:
        app: ignite
    spec:
      terminationGracePeriodSeconds: 60000
      containers:
        # Custom pod name.
      - name: ignite-node
        # Limits and requests for the Ignite container.
        resources:
          limits:
            cpu: "4"
            memory: 4Gi
          requests:
            cpu: "4"
            memory: 4Gi
        env:
          # Must be specified to ensure that Apache Ignite 3 cluster replicas are visible to each other.
          - name: IGNITE_NODE_NAME
            valueFrom:
              fieldRef:
                fieldPath: metadata.name
          # Apache Ignite 3 working directory.
          - name: IGNITE_WORK_DIR
            value: /ai3-work
        # Apache Ignite Docker image and its version.
        image: apache/ignite3:{version}
        ports:
        - containerPort: 10300
        - containerPort: 10800
        - containerPort: 3344
        volumeMounts:
        # The config will be placed at this path in the container.
        - mountPath: /opt/ignite/etc/ignite-config.conf
          name: config-vol
          subPath: ignite-config.conf
        # Ignite 3 working directory.
        - mountPath: /ai3-work
          name: persistence
      volumes:
      - name: config-vol
        configMap:
          name: ignite-config
  volumeClaimTemplates:
  - apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: persistence
    spec:
      accessModes:
      - ReadWriteOnce
      resources:
        requests:
          storage: 10Gi # Provide enough space for your application data.
      volumeMode: Filesystem
```

2. Apply the `statefulset.yaml` file to deploy the main components of Apache Ignite 3:

```shell
kubectl apply -f statefulset.yaml
```

### Wait for Pods to Start

1. Monitor the status of the pods:

```shell
kubectl get pods -n <namespace> -w
```

2. Ensure that all pods' `STATUS` is `Running` before proceeding.

### Deploy the Job

1. Prepare the `job.yaml` file for deploying the job:

```yaml title="job.yaml"
apiVersion: batch/v1
kind: Job
metadata:
  name: cluster-init
  # Place your namespace name here.
  namespace: <namespace>
spec:
  template:
    spec:
      containers:
      # Command to init the cluster. URL and host must be the name of the service you created before. Port is 10300 as the management port.
      - args:
        - -ec
        - |
          apt update && apt-get install -y bind9-host
          IGNITE_NODES=$(host -tsrv _cluster._tcp.ignite-svc-headless | grep 'SRV record' | awk '{print $8}' | awk -F. '{print $1}' | paste -sd ',')
          /opt/ignite3cli/bin/ignite3 cluster init --name=ignite --url=http://ignite-svc-headless:10300
        command:
        - /bin/sh
        # Specify the Docker image with the Apache Ignite 3 CLI and its version.
        image: apache/ignite3:{version}
        imagePullPolicy: IfNotPresent
        name: cluster-init
        resources: {}
      restartPolicy: Never
      terminationGracePeriodSeconds: 120
```

2. Apply the `job.yaml` file to complete installation:

```shell
kubectl apply -f job.yaml
```

## Installation Verification

1. Check the status of all resources in your namespace:

```shell
kubectl get all -n <namespace>
```

2. Ensure that all components are running as expected, without errors, and that the initialization job is in the `Completed` status.
3. Verify that your cluster is initialized and running:

```shell
kubectl exec -it ignite-cluster-0 bash -n <namespace>
/opt/ignite3cli/bin/ignite3 cluster status
```

The command output must include the name of your cluster and the number of nodes. The status must be `ACTIVE`.

## Installation Troubleshooting

If any issues occur during the installation:

- Check the logs of specific pods:

```shell
kubectl logs <pod-name> -n <namespace>
```

- Review events in the namespace:

```shell
kubectl get events -n <namespace>
```
