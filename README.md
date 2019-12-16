# Accordia: Adaptive Cloud Configuration Optimization for Recurring Data-Intensive Applications
Recognizing the diversity of big data analytic jobs, cloud providers offer a wide range of virtual machine (VM) instances or even clusters to cater for different use cases. The choice of cloud instance configurations can have a significant impact on the response time and running cost of data-intensive, production batch-jobs, which need to be re-run regularly using cloud-scale resources. However, identifying the best cloud configuration with a low search cost is quite challenging due to i) the large and high-dimensional configuration-parameters space, ii) the dynamically varying price of some instance types (e.g. spot-price ones), iii) job execution-time variation even given the same configuration, and iv) gradual drifts / unexpected changes of the characteristics of a recurring job. To tackle these challenges, we have designed and implemented Accordia, a system that enables Adaptive Cloud Configuration Optimization for Recurring Data-Intensive Applications. By leveraging recent algorithmic advances in Gaussian Process UCB (Upper Confidence Bound) techniques, the design of Accordia can handle time-varying instance pricing while providing a performance guarantee of sub-linearly increasing regret when comparing with the static, offline optimal solution. Using extensive trace-driven simulations and empirical measurements of our Kubernetes-based implementation, we demonstrate that Accordia can dynamically learn a near-cost-optimal cloud configuration (i.e. within 10\% of the optimum) after fewer than 20 runs from over 7000 candidate choices within a 5-dimension search space, which translates to a 2X-speedup and a 20.9\% cost-savings, when comparing to CherryPick.


# Online Documentation
The full technical report is available at [http://mobitec.ie.cuhk.edu.hk/cloudComputing/Accordia.pdf ](http://mobitec.ie.cuhk.edu.hk/cloudComputing/Accordia.pdf).


# Prerequisites
We build Accordia system for Spark applications on top of Kubernetes in Google Cloud. Following is the prerequisites for Accordia system.

* Ubuntu 16.04
* Java JDK 1.8
* gcloud, gsutil
* Kubernetes 1.8 or above
* Spark 2.3.0 or above


# Setup
If you want to run the benchmarking Spark applications (i.e. SparkPi, PageRank and WordCount) of this repository, please use the following guide for setup and usage:

1) Install sbt
We use sbt to compile Spark Scala program. You can use following command lines to install sbt.
``` 
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt/list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt
```

2) Use sbt to compile Spark Scala program
Following is the example to use sbt compiling Spark PageRank program.
```
mkdir ./pagerank
mkdir -p ./pagerank/src/main/scala     
vim ./pagerank/src/main/scala/SparkPageRank.scala
vim ./pagerank/simple.sbt
cd pagerank
find .
sbt package
```

2) Build Spark Docker Image
Spark 2.3.0 provides an docker template in the file spark/kubernetes/dockerfilrs/spark/Dockerfile and provides tools to build spark docker images in the file spark/bin/docker-image-tool.sh . Following is the example to build Spark PageRank docker.
```
\\Add following lines in Dockerfile to specify Pagerank program and Pagerank Data
COPY pagerank /opt/spark/pagerank
COPY data /opt/spark/data
\\Submit PageRank docker image in Google Container Registry
sudo bin/docker-image-tool.sh -r gcr.io/[google_container_registry_name] -t [my_tag] build
sudo bin/docker-image-tool.sh -r gcr.io/[google_container_registry_name] -t [my_tag] push
\\Public Pagerank docker image
gsutil iam ch allUsers:objectViewer gs://artififacts.[google_container_registry_name].appspot.com
```

3) Kubernetes RBAC
In Kubernetes clusters with RBAC enabled, users can configure Kubernetes RBAC roles and service accounts used by the various Spark on Kubernetes components to access the Kubernetes API server.
```
kubectl get serviceaccount
kubectl get clusterrolebinding
kubectl create serviceaccount spark
kubectl create clusterrolebinding  spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
kubectl get serviceaccount 
```

4) Discover Kubernetes apiserver URL
If you have a Kubernetes cluster setup, one way to discover the apiserver URL is by executing kubectl cluster-info.
```
kubectl cluster-info
```

5) Launch Spark Program in Kubernetes
The Spark master, specified either via passing the --master command line argument to spark-submit or by setting spark.master in the application’s configuration, must be a URL with the format k8s://[kubernetes_apiserver_url]. Prefixing the master string with k8s:// will cause the Spark application to launch on the Kubernetes cluster, with the API server being contacted at [kubernetes_apiserver_url], which can be obtained in the previous step. You can launch PageRank Spark application in Kubernetes with the following command line:
```
bin/spark-submit \
     --master k8s://[kubernetes_apiserver_url] \
     --deploy-mode cluster \
     --name pagerank \
     --class org.apache.spark.examples.PageRank \
     --conf spark.app.name=pagerank \
     --conf spark.kubernetes.authenticate.diver.serviceAccountName=spark \
     --conf spark.kubernetes.container.image=gcr.io/[google_container_registry_name]/spark:[my_tag] \
     local:///opt/spark/examples/jars/spark-examples_2.11-2.3.0.jar \
     100000
```


# Accordia Usage
1) Prepare Spark job configuration
In order to use Accordia system selecting the optimal cloud configuration, you need to prepare the file spark_job_configure.py to specify the parameter to launch the Spark application. Following is the example of spark_job_configure.py for PageRank.
```
SPARK_HOME = '/usr/local/spark'
MASTER_NODE = 'k8s://[kubernetes_apiserver_url]'
job_name = 'pagerank'
class_name = 'org.apache.spark.examples.JavaSparkPi'
spark_app_name = 'pagerank'
container_image = 'gcr.io/[google_container_registry_name]/spark:[my_tag]'
java_code = 'local:///opt/spark/examples/jars/spark-examples_2.11-2.3.0.jar 100'
```

2) Accordia without abort mechanism
Launch the Spark application via Accordia System without abort mechanism.
```
python3 src/Accordia_without_Abort.py
```

3) Accordia with abort mechanism
To avoid the waste of resources for the obvious poor-performance cloud configuration, Accordia can abort the job, as long as its estimated job completion cost is 30% larger than the best solution observed so far. Launch the Spark application via Accordia System without abort mechanism.
```
python3 src/Accordia_with_Abort.py
```


# Hints  
1) Spark Diver Pod log
You can check the log in the Spark Driver Pod to debug via the following command line.
```
kubectl get pods
kubectl logs -f pagerank-xxxxxxxxxx-driver
```

2) Waiting for pulling the image
It is due to Kubernetes Spark Diver Pod can not download the docker image from the Google Container Registry. Make sure your registry public via the following command line.
```
gsutil iam ch allUsers:objectViewer gs://artififacts.[google_container_registry_name].appspot.com
```

3) Cannot create pod
It is due to Spark Driver Pod does not have permission to create Spark Executor pods. Make sure Kubernetes clusters with RBAC enabled.
```
kubectl create serviceaccount spark
kubectl create clusterrolebinding  spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
```
