# Apache Livy for k8s spark 3.3.0

I used a fork of the project https://github.com/apache/incubator-livy (include PR: https://github.com/apache/incubator-livy/pull/249/files)

And also some of the ideas of the guys from the project https://github.com/HPEEzmeral/spark-on-k8s

In the current project, I added a Dokerfile to build livy, spark, as well as helm chart to run in the k8s environment

The scheme of work is similar to the scheme in the Hadoop cluster:
![scheme](images/livy-k8s.png)


## Building Livy

To build, it is enough to build the Dockerfile in the root of the project.
```
docker build -t livy .
```

## Install Livy

You need to create a Namespace spark in the l8y cluster

```
kubectl create ns spark
```

Then install helm chart:
```
cd helm/
helm upgrade --install livy . -n spark
```

## Example
After the pod is launched. It is necessary to organize access to port 8998. It does not matter how it will be done, through the forwarding port or intgerss.

To start a session, you need to send a POST request http://localhost:8998/sessions.
```
# Example
{"kind": "spark" , "driverMemory": "8g",    "driverCores": 4,    "executorMemory": "4g",     "executorCores": 1,     "numExecutors": 10}
```
![scheme](images/livy.png)

Next you will see the formation of spark pods:
![scheme](images/spark-pod.png)

## Jupyter

To work from Jupyter, you need to create kernels (https://github.com/jupyter-incubator/sparkmagic)
![img.png](images/jupyter.png)


## This scheme of work is at the testing stage, I am not yet sure about its advantages!