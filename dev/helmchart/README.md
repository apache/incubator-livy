# Apache Livy Deployed on Kubernetes using Docker Desktop

This guide provides a Helm chart to deploy Livy on Kubernetes without relying on cloud services like AWS, GCP, or Azure. This setup can save development time and cost, and it allows debugging using an IDE. For debugging Livy on Kubernetes as a standalone setup, Apache Spark and Apache Livy must be deployed in Kubernetes.

## Docker Desktop Installation
1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
2. Enable Kubernetes in Docker Desktop settings.

## Helm Installation
1. Install [Helm](https://helm.sh/docs/intro/install/).
2. Add the required Helm chart repositories:
    ```shell
    helm repo add cert-manager https://charts.jetstack.io              
    helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
    helm repo update
    ```

3. Add an entry to the `/etc/hosts` file:
    ```text
    127.0.0.1 my-cluster.example.com
    ```

4. Install the cert-manager CustomResourceDefinition resources:
    ```shell
    kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.15.0/cert-manager.yaml
    ```

**References:**
- [cert-manager ACME nginx-ingress tutorial](https://cert-manager.io/docs/tutorials/acme/nginx-ingress/)
- [Artifact Hub cert-manager Helm chart](https://artifacthub.io/packages/helm/cert-manager/cert-manager)

## Livy Cluster Deployment
1. Build the Helm chart using the following command:
    ```bash
    helm dependency build
    ```

2. Create a Kubernetes namespace for the Livy deployment:
    ```bash
    kubectl create namespace <namespace-name>
    ```

3. Install the Livy cluster using the Helm chart:
    ```bash
    helm -n <namespace-name> install livycluster .
    ```

## Livy REST API Testing
1. Create an interactive session:
    ```shell
    curl -k -X POST -H "Content-Type: application/json" --data '{"kind": "spark"}' https://my-cluster.example.com/livy/sessions | jq
    ```
   **Note:** You need `curl` and `jq` utilities installed on your local machine for testing.

2. Create a statement:
    ```shell
    curl -k -X POST -d '{ "kind": "spark", "code": "sc.parallelize(1 to 10).count()" }' -H "Content-Type: application/json" \
    https://my-cluster.example.com/livy/sessions/0/statements | jq
    ```

3. Create a batch job:
    ```shell
    curl -s -k -H "Content-Type: application/json" \
        -X POST \
        -d '{
            "name": "testbatch1",
            "className": "org.apache.spark.examples.SparkPi",
            "numExecutors": 2,
            "file": "local:///opt/spark/examples/jars/spark-examples_2.12-3.2.3.jar",
            "args": ["10000"]
        }' "https://my-cluster.example.com/livy/batches" | jq
    ```

## Steps to Create Docker Images

Steps to create Docker images for Spark and Livy are documented at [Docker.md](Docker.md).