Streaming Data Pipeline and ML Model with Kafka and PyTorch (A CSE6250 Project)
============

# Demo
In the video below, you can see the output from the User Interface, which receives and displays in real time the following signals:
- raw streaming data
- preprocessed streaming data, 
- the prediction from the ML model.

We also have an presentation for this project.
[![Streaming Data Pipeline and ML Model with Kafka and PyTorch (A CSE6250 Project)](https://img.youtube.com/vi/R6znyW2okTg/0.jpg)](https://youtu.be/R6znyW2okTg "Streaming Data Pipeline and ML Model with Kafka and PyTorch (A CSE6250 Project)")
# Authors
- [Travis Tang](https://www.linkedin.com/in/travistang)
- [Kento Ohtani](https://jp.linkedin.com/in/kento-ohtani-67385a148)
# Summary
We reproduced a streaming data pipeline from the paper "Silent trial protocol for evaluation of real-time models at ICU". The pipeline that uses deep learning to evaluate the risk of cardiac arrest with waveform data in real time. The streaming pipeline, orchestrated with Docker Compose, performs the following.
1. Creates streaming data of physiological signals from MIMIC III Dataset.
2. Sends the raw streaming data using Kafka.
3. Preprocesses the raw data using PySpark.
4. Predicts if patient is at risk of cardiac arrest using PyTorch.
5. Stores the results in MySQL.
6. Displays the data and results in real-time using Bokeh.

# Instructions for running
1. Pull this repository into your local.
2. Install docker locally. Then, pull the docker image `btctothemoonz/travisnkento:008` with the command `docker pull btctothemoonz/travisnkento:008`.
3. Make sure you pull all data, including the data stored in the `data` directory. In particular the directory with 
- data/waveform/physionet.org/p00/p000194
- data/waveform/physionet.org/p04/p004980
4. On a command line prompt, use `cd` to navigate to this directory.
5. Then, run `docker compose up`. After this command, Kafka, Zookeeper, MySQL, the preprocessing container, the graph plotting container and the deeplearning container are starting. 
6. Please start monitoring the logs on docker compose. You should see a line that repeats every second that states “Countdown 60 seconds: Please manually start a browser http://localhost:5068/ else the server will crash.” Please start a browser and navigate to the link http://localhost:5068/ before the server crashes. Please DO NOT refresh this page upon loading it. Please wait for up to three minutes before the first data point shows up.
You should then see the log “Thank you for going to http://localhost:5068/!” within the logs.
Then, you can sit back and watch the raw and processed data and predictions appear in http://localhost:5068/.
- Initially, only raw data will appear.
- Starting at the 3rd minute, processed data will appear.
- Starting at the 10th minute, predictions will appear.
7. You can now wait and see the results being streamed. 