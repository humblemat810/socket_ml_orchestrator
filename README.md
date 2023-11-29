# socket_ml_orchestrator
socket supported orchestrator using fast socket transfer data between orchestrator and worker designed for Machine learning workload

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

To dos  
 1. Framework for the task queuer to work on task directly if it has capacity.
 2. More test cases such as gpu workload tests