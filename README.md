# Python Ray Remote Actors

__Remote Actors__ allow to make parallel remote procedure calls just like [Remote Functions](https://mpolinowski.github.io/docs/IoT-and-Machine-Learning/AIOps/2023-01-23-python-ray-remote-functions/2023-01-23). But unlike the latter they enable you to maintain a state between invocations. To ensure state consistency, actors process one request at a time. An actor can:

* Store data
* Receive messages from other actors
* Pass messages to other actors
* Create additional child actors


> Source: [Scaling Python with Ray](https://github.com/scalingpythonml/scaling-python-with-ray)
