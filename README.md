
# socket_ml_orchestrator
socket supported orchestrator using fast socket transfer data between orchestrator and worker designed for Machine learning workload
### Work on stackless python, pypy, and cpython, windows and Xnux

SEO keywords: Windows-Celery, Task queue, TASKQML, 

[Installation](README.md#installation)

[Benchmark](README.md#Benchmark)

# Design Background/ Overview: 
Machine learning projects often use python and currenctly it lacks a python native solution to stream realtime speedy output for small to medium scale solutions

Most of the solutions like gradio use RESTAPI
Celery does not have a native in memory task queue. It at least use light weight redis.
And Celery decoupled producer and consumer.

In many real time applications like video, what it requires is a few worker to improve frame rate.

The designed target application is video streaming with machine learning applied to the frames in real time. A central coordinator will directly work with a few workers while itself will arrange the taskqueue
such that the task queue status IO will time will be saved. However, the design has also bear in mind that the taskq operations will be easily migrated to redis rabbitmq like medium speed external queues (as compare to direct in-program memory and slow database calls), Kafka is heavy weight and need a JVM separately running.
Similarly, pyspark is heavy weight and require tedious config and it is not designed for real-time application and it works for minibatches.

Apache flink has bad developer support so far, following official document not even able to set up. Possibly dockerised container may work in flink, which is too heavy weight that requires docker + JVM + no support. Flink has subset of full feature when use python instead of scala or java. If single docker is already considered heavyweight, need not mention solutions that use kubernetes or zookeepers to be overly complicated and resource heavy.

So the market now lacks an opensource python light weight light code no package dependency(vanilla use, not example demo cases) performant framework for real time machine learning based streaming applications.

This solution directly receieve data from input, self arrange workers, and output streams of results.


A task worker will have the worker-side and server-side implementation. Server side is mainly to tell how to 
 1. preprocess the input and output stream to and from external sources
 2. the data serialisation from server to worker

Users will need to subclass the server side and the client side to work

The current framework / inheritance tree is as follows:
 base worker > base socket worker > application specific worker

A simple self shutdown simplehttpd is used to shutdown the worker just in case, using the route /shutdown

# Benchmarking

A random word count test case is made. Each word is sent to the worker and a counter has to be returned
However, celery convert the results into json and it cannot serialise custom objects. So to make the comparison fair, the returned dict is converted into counter before adding.


Against celery:
using 7 workers on pytaskqml the results are:
##
banana 904  
apple 950  
orange 967  
grape 929  
kiwi 947  
cherry 933  

and using celery results are:
##
grape 722  
apple 701  
cherry 726  
kiwi 728  
banana 709  
orange 722  

Moring, with a bit trial and error, using 2 in memory local thread worker
plus 2 socket worker, the so far best result is 3 times the redis celery result


##
orange 1804  
apple 1766  
cherry 1798  
banana 1872  
kiwi 1833  
grape 1911  

Interestingly, using only local worker with 7 workers,
the result is much worse:

##
kiwi 409  
grape 382  
cherry 410  
orange 375  
banana 396  
apple 380  

For this particular test case, pytaskqml outruns celery.
To dos  
 1. Framework for the task queuer to work on task directly if it has capacity.
 2. More test cases such as gpu workload tests

<a id="installation"></a>
# Installation

Plain use of the taskqml requires no additional dependency other than the python itself.
An empty requirements.txt is provided.

But to develop and to run the examples, other dependencies are used as an illustration. Also, to run a simple benchmark against Celery, celery itself will be required.

The demo environment would require the demo to be run alone with the pytaskqml separately installed.

The development requires developing and testing the demo. So it is recommended to install pytaskqml in editable mode using pip install -e \<pytaskml root\>