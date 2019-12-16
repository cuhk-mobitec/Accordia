import os
import csv
import time
import math
import copy
import random
import scipy.stats
import numpy as np
import pandas as pd
from math import sqrt
from math import log
from sklearn import preprocessing
from scipy.spatial.distance import pdist, squareform
from spark_job_configure import job_name,class_name,spark_app_name,container_image,java_code, \
    SPARK_HOME, MASTER_NODE


initial_driver_cores = '2'
initial_driver_memory = '4G'
initial_executor_num= '3'
initial_executor_cores = '3'
initial_executor_memory = '8G'

max_driver_cores = '3'
max_driver_memory = '14G'
max_executor_num = '4'
max_executor_cores = '3'
max_executor_memory = '14G'


def get_spark_command(SPARK_HOME, MASTER_NODE, job_name, class_name, spark_app_name, \
    container_image,java_code, driver_cores, driver_memory, executor_num, executor_cores, \
    executor_memory):
    spark_command = SPARK_HOME + '/bin/spark-submit --master ' + MASTER_NODE + \
        ' --deploy-mode cluster --name ' + job_name + ' --class ' + class_name + \
        ' --conf spark.driver.cores=' + driver_cores + ' --conf spark.driver.memory=' + \
        driver_memory + ' --conf spark.executor.instances=' + executor_num + ' --conf spark.executor.cores=' + \
        executor_cores + ' --conf spark.executor.memory=' + executor_memory + ' --conf spark.app.name=' + \
        spark_app_name + ' --conf spark.kubernetes.container.image=' + container_image + ' ' + java_code
    #print(spark_command)
    return spark_command


def store_record(job_name, driver_cores, driver_memory, executor_num, executor_cores, \
    executor_memory, process_time):   
    job_record = job_name + '\t' + driver_cores + '\t' + driver_memory + '\t' + \
        executor_num + '\t' + executor_cores + '\t' +executor_memory + '\t' + str(process_time) + '\n'
    if os.path.exists(job_name):
        job_history = open(job_name, 'a')
    else:
        job_history = open(job_name, 'w')
    job_history.write(job_record)
    job_history.close()


# wait to connect with spot price
def per_dollar_computing_ratio(gaussian_info):
    info = gaussian_info.copy()
    for i in range(len(info[:,-1])):
        #info[i,-1] = 3600.0 / (info[i,-1] * info[i,0] * info[i,2] * info[i,3])
        info[i,-1] = 3600.0 / (info[i,-1] * (info[i,0] + info[i,1]/4 +info[i,2] * (info[i,3]+info[i,4]/4)))
    return info[:,-1]


# Using Gaussian Process Bandit Algorithm to Select the Cloud Configuration
def gaussian_process_bandit(job_name):    
    sigma = 0.1
    l = 100
    # driver_cpu, driver_memory, executor_num, executor_cpu, executor_memory, running_time
    gaussian_info = []
    count = 0
    with open(job_name,'r') as job_history:
        while True:
            line = job_history.readline()
            if not line:
                break
            line = line.strip()
            words = line.split('\t')
            if len(words) == 7:
                gaussian_info.append([float(words[1]), float(words[2][0:-1]), float(words[3]), \
                    float(words[4]), float(words[5][0:-1]), float(words[6])]) 
                count = count + 1
        gaussian_info = np.array(gaussian_info, dtype = float)


    # driver_cpu, driver_memory, executor_num, executor_cpu, executor_memory
    parameter = gaussian_info[:,:-1]
    # the per dollar computing ratio corresponding to each cloud configuration
    performance = per_dollar_computing_ratio(gaussian_info)
    performance = preprocessing.scale(performance)


    # id, driver_cpu, driver_memory, executor_num, executor_cpu, executor_memory,
    candidate_configuration = []
    num_candidate = 0
    for i in range(int(max_driver_cores)):
        for j in range(3,int(max_driver_memory[0:-1])):
                for k in range(int(max_executor_num)):
                    for l in range(int(max_executor_cores)):
                        for m in range(3,int(max_executor_memory[0:-1])):
                            candidate_configuration.append([num_candidate, i+1, j+1, k+1,l+1, \
                                m+1 ])
                            num_candidate = num_candidate + 1
 
    #print(candidate_configuration[1245])
    large_tmpk = np.exp(-squareform(pdist(np.array(parameter),'euclidean'))**2 / (2 * l**2))
    inverse_tmpk = np.linalg.inv(large_tmpk + sigma**2 * np.eye(count))
    tmp_mu = inverse_tmpk.dot(np.array(performance))


    # compute the performance expectation and variance for all candidate cloud configuration
    # mu, var, gp_score for all candidate configuration
    ratting = np.zeros((num_candidate,3), dtype = float)
    for j in range(num_candidate): 
        small_tmpk = np.exp(-np.sum(np.abs((candidate_configuration[j][1:6] + \
            np.zeros((count,5))) - np.array(parameter))**2, axis = -1) / (2* l **2))
        mu = small_tmpk.dot(tmp_mu)
        var = sqrt(1-small_tmpk.dot(inverse_tmpk.dot(small_tmpk)))
        ratting[j,:] = [mu,var, mu + sqrt(2*log((i * math.pi)**2 / 6 / 0.05))*var/10]

    flag = np.argmax(ratting[:,2])
    #print(candidate_configuration[flag])

    driver_cores = str(candidate_configuration[flag][1])
    driver_memory = str(candidate_configuration[flag][2]) + 'G'
    executor_num = str(candidate_configuration[flag][3])
    executor_cores = str(candidate_configuration[flag][4])
    executor_memory = str(candidate_configuration[flag][5]) + 'G'
    
    return driver_cores, driver_memory, executor_num, executor_cores, executor_memory


# Using Cherry Algorithm to Select the Cloud Configuration
def cherrypick(job_name):
    sigma = 0.1
    l = 100
    epsilon = 0.01
    # driver_cpu, driver_memory, executor_num, executor_cpu, executor_memory, running_time
    gaussian_info = []
    count = 0
    with open(job_name,'r') as job_history:
        while True:
            line = job_history.readline()
            if not line:
                break
            line = line.strip()
            words = line.split('\t')
            if len(words) == 7:
                gaussian_info.append([float(words[1]), float(words[2][0:-1]), float(words[3]), \
                    float(words[4]), float(words[5][0:-1]), float(words[6])]) 
                count = count + 1
        gaussian_info = np.array(gaussian_info, dtype = float)    
        
    if count == 1:
        driver_cores = max_driver_cores
        driver_memory = max_driver_memory
        executor_num = max_executor_num
        executor_cores = max_executor_cores
        executor_memory = max_executor_memory
        return driver_cores, driver_memory, executor_num, executor_cores, executor_memory
    elif count == 2:
        driver_cores = '1'
        driver_memory = '1G'
        executor_num = '1'
        executor_cores = '1'
        executor_memory = '1G'
        return driver_cores, driver_memory, executor_num, executor_cores, executor_memory   


    # driver_cpu, driver_memory, executor_num, executor_cpu, executor_memory
    parameter = gaussian_info[:,:-1]
    # the per dollar computing ratio corresponding to each cloud configuration
    performance = per_dollar_computing_ratio(gaussian_info)
    performance = preprocessing.scale(performance)


    # id, driver_cpu, driver_memory, executor_num, executor_cpu, executor_memory,
    candidate_configuration = []
    num_candidate = 0
    for i in range(int(max_driver_cores)):
        for j in range(3,int(max_driver_memory[0:-1])):
                for k in range(int(max_executor_num)):
                    for l in range(int(max_executor_cores)):
                        for m in range(3,int(max_executor_memory[0:-1])):
                            candidate_configuration.append([num_candidate, i+1, j+1, k+1,l+1, \
                                m+1 ])
                            num_candidate = num_candidate + 1
 
    #print(candidate_configuration[1245])
    large_tmpk = np.exp(-squareform(pdist(np.array(parameter),'euclidean'))**2 / (2 * l**2))
    inverse_tmpk = np.linalg.inv(large_tmpk + sigma**2 * np.eye(count))
    tmp_mu = inverse_tmpk.dot(np.array(performance))
    max_performance = max(performance)


    # compute the performance expectation and variance for all candidate cloud configuration
    # mu, var, gp_score for all candidate configuration
    ratting = np.zeros((num_candidate,3), dtype = float)
    for j in range(num_candidate): 
        small_tmpk = np.exp(-np.sum(np.abs((candidate_configuration[j][1:6] + \
            np.zeros((count,5))) - np.array(parameter))**2, axis = -1) / (2* l **2))
        mu = small_tmpk.dot(tmp_mu)
        var = sqrt(1-small_tmpk.dot(inverse_tmpk.dot(small_tmpk)))
        z = (mu-max_performance-epsilon)/var
        bayesian_optimization_score = (mu-max_performance-epsilon)*scipy.stats.norm(0,1).cdf(z) + \
            var*scipy.stats.norm(0,1).pdf(z)
        ratting[j,:] = [mu,var, bayesian_optimization_score]

    flag = np.argmax(ratting[:,2])
    #print(candidate_configuration[flag])


    driver_cores = str(candidate_configuration[flag][1])
    driver_memory = str(candidate_configuration[flag][2]) + 'G'
    executor_num = str(candidate_configuration[flag][3])
    executor_cores = str(candidate_configuration[flag][4])
    executor_memory = str(candidate_configuration[flag][5]) + 'G'
    
    return driver_cores, driver_memory, executor_num, executor_cores, executor_memory


if __name__ == '__main__':

    if os.path.exists('spark_job_configure.py'):

        if os.path.exists(job_name):
            print('I find that '+ job_name + ' has already processed.')
            driver_cores, driver_memory, executor_num, executor_cores, executor_memory = \
                gaussian_process_bandit(job_name)
            #driver_cores, driver_memory, executor_num, executor_cores, executor_memory = cherrypick(job_name)
            spark_command = get_spark_command(SPARK_HOME, MASTER_NODE, job_name, class_name, \
                spark_app_name, container_image,java_code, driver_cores, driver_memory, executor_num, \
                executor_cores, initial_executor_memory)
            print('We want to try the cloud configuration with '+ driver_cores + '-CPU '+ \
                driver_memory + '-memory driver and ' + executor_num + ' ' +  executor_cores + '-CPU '+ \
                executor_memory + '-memory executor.')
            #arrive_time = time.time()
            #os.system(spark_command)
            #process_time = time.time() - arrive_time
            #store_record(job_name, driver_cores, driver_memory, executor_num, executor_cores, executor_memory, process_time)
                   
        else:
            print('job '+ job_name + ' is the first time to process.')
            spark_command = get_spark_command(SPARK_HOME, MASTER_NODE, job_name, class_name, \
                spark_app_name, container_image,java_code, initial_driver_cores, initial_driver_memory, \
                initial_executor_num, initial_executor_cores, initial_executor_memory)
            print('We want to try the cloud configuration with '+ initial_driver_cores + '-CPU '+ \
                initial_driver_memory + '-memory driver and ' + initial_executor_num + ' ' +  \
                initial_executor_cores + '-CPU '+ initial_executor_memory + '-memory executor.')
            arrive_time = time.time()
            os.system(spark_command)
            process_time = time.time() - arrive_time
            store_record(job_name, initial_driver_cores, initial_driver_memory, initial_executor_num, \
                initial_executor_cores, initial_executor_memory, process_time)
    else:
        print('Please prepare your spark job in the spark_job_configure file.')       


# os.system('/usr/local/spark/bin/spark-submit --master k8s://35.226.10.213 --deploy-mode cluster --name spark-pi --class org.apache.spark.examples.JavaSparkPi --conf spark.driver.cores=2 --conf spark.driver.memory=4G --conf spark.executor.instances=3 --conf spark.executor.cores=3 --conf spark.executor.memory=8G --conf spark.app.name=sparkpi --conf spark.kubernetes.container.image=gcr.io/kubernetessparkautotune/spark:k8s-spark-2.3.0 local:///opt/spark/examples/jars/spark-examples_2.11-2.3.0.jar 1000')
# kubectl create clusterrolebinding  spark-role --clusterrole=edit --serviceaccount=default:default --namespace=default