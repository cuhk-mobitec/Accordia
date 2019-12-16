# Accordia: Adaptive Cloud Configuration Optimization for Recurring Data-Intensive Applications
Recognizing the diversity of big data analytic jobs, cloud providers offer a wide range of virtual machine (VM) instances or even clusters to cater for different use cases. The choice of cloud instance configurations can have a significant impact on the response time and running cost of data-intensive, production batch-jobs, which need to be re-run regularly using cloud-scale resources. However, identifying the best cloud configuration with a low search cost is quite challenging due to i) the large and high-dimensional configuration-parameters space, ii) the dynamically varying price of some instance types (e.g. spot-price ones), iii) job execution-time variation even given the same configuration, and iv) gradual drifts / unexpected changes of the characteristics of a recurring job. To tackle these challenges, we have designed and implemented Accordia, a system that enables Adaptive Cloud Configuration Optimization for Recurring Data-Intensive Applications. By leveraging recent algorithmic advances in Gaussian Process UCB (Upper Confidence Bound) techniques, the design of Accordia can handle time-varying instance pricing while providing a performance guarantee of sub-linearly increasing regret when comparing with the static, offline optimal solution. Using extensive trace-driven simulations and empirical measurements of our Kubernetes-based implementation, we demonstrate that Accordia can dynamically learn a near-cost-optimal cloud configuration (i.e. within 10\% of the optimum) after fewer than 20 runs from over 7000 candidate choices within a 5-dimension search space, which translates to a 2X-speedup and a 20.9\% cost-savings, when comparing to CherryPick.


# Online Documentation
The full technical report is available at [http://mobitec.ie.cuhk.edu.hk/cloudComputing/Accordia.pdf ](http://mobitec.ie.cuhk.edu.hk/cloudComputing/Accordia.pdf).

# Setup
If you want to run the benchmarking Spark applications (i.e. SparkPi, PageRank and WordCount) of this repository, please use the following guide for setup and usage:

* Kubernetes
