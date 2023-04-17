time-series-kafka-demo
============

Instructions for running this.
1. Pull this repository into your local.
2. Install docker locally. Then, pull the docker image `btctothemoonz/travisnkento:008` with the command `docker pull btctothemoonz/travisnkento:008`.
3. Make sure you pull all data, including the data stored in the `data` directory. In particular the directory with 
- data/waveform/physionet.org/p00/p000194
- data/waveform/physionet.org/p04/p004980
4. On a command line prompt, use `cd` to navigate to this directory.
5. Then, run `docker compose up`. At this point, a few things are happening
- Kafka, Zookeeper, MySQL, the preprocessing container, the graph plotting container and the deeplearning container are starting. 
- In the log, you will see a line of log that says "http://localhost:5068/". Please open up a browser in and navigate to the "http://localhost:5068/" upon seeing this message. Else, the plotting component of this project will fail. 
- Please DO NOT refresh this page upon loading it.
6. You can now wait and see the results being streamed. 