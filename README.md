
# socket_ml_orchestrator
socket supported orchestrator using fast socket transfer data between orchestrator and worker designed for Machine learning workload
### Work on stackless python, pypy, and cpython, windows and Xnux

SEO keywords: Windows-Celery, Task queue, TASKQML, 

[Design Background/ Overview](#overview)

[Benchmark](#benchmark)

[Installation](#installation)

<a id="overview"></a>
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

<a id="benchmark"></a>
# Benchmarking Data

A random word count test case is made. Each word is sent to the worker and a counter has to be returned
However, celery convert the results into json and it cannot serialise custom objects. So to make the comparison fair, the returned dict is converted into counter before adding.



using 7 workers on pytaskqml the results are (demo_case3_n_workers.py):
##
kiwi 1144
banana 1169
cherry 1159
grape 1185
orange 1136
apple 1166,

no profiler
grape 1357
kiwi 1368
banana 1462
apple 1409
cherry 1356
orange 1417
Against celery:
and using celery results are: (celery_redis_word_count.py, celery_task_create.py)
##
grape 722  
apple 701  
cherry 726  
kiwi 728  
banana 709  
orange 722  

Moring, with a bit trial and error, using 2 in proc local thread worker
plus 2 socket worker, the result is 3 times the redis celery result
(demo_case6_mixed_worker_no_profile.py)

##
banana 2048 
cherry 1964 
orange 1978 
kiwi 1987 
grape 2087 
apple 2005 

When yappi is turned on (demo_case5_mixed_worker_type.py)
the results become:
grape 1621 
cherry 1680 
kiwi 1688 
apple 1639 
banana 1599 
orange 1639 

Interestingly, using only local worker with 7 workers,
the result is much worse with yappi profiler on: (demo_case4_local_worker_only.py)

##
grape 901 
apple 942 
cherry 930 
kiwi 920 
banana 922 
orange 905 

however, with yappi profiler on: 

apple 1997 
kiwi 2067 
orange 2014 
grape 2000 
banana 2046 
cherry 2027 

With input using socket receive instead of direct insert into the queue, the speed is a bit slower

kiwi 1670 
banana 1674 
orange 1630 
grape 1736 
cherry 1651 
apple 1727 

and again, speed increases with profiler off 

cherry 1739 
grape 1741 
apple 1784 
banana 1754 
orange 1732 

For this particular test case, pytaskqml outruns celery.
To dos  
 1. Framework for the task queuer to work on task directly if it has capacity.
 2. More test cases such as gpu workload tests

<a id="installation"></a>

# Installation

Plain use of the taskqml requires no additional dependency other than the python itself.
An empty requirements.txt is provided.

```pip install -r dev_requirements.txt```

But to develop and to run the examples, other dependencies are used as an illustration. Also, to run a simple benchmark against Celery, celery itself will be required.

```pip install -r demo-requirements.txt```

The demo environment would require the demo to be run alone with the pytaskqml separately installed.

The development requires developing and testing the demo. So it is recommended to install pytaskqml in editable mode using pip install -e \<pytaskml root\>