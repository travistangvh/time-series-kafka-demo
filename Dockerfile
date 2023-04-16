# Following https://levelup.gitconnected.com/how-to-run-spark-with-docker-c6287a11a437

# These are the commands used to build this: btctothemoonz/travisnkento-pytorch:0.0.1
# docker login
# docker build -t btctothemoonz/travisnkento:0.0.1 .
# docker push btctothemoonz/travisnkento:0.0.1

FROM btctothemoonz/travisnkento:0.0.7
ENV REFRESHED_AT 2021-08-15

ADD requirements.txt /home/requirements.txt
ADD .env /home/.env
# RUN pip install -r /home/requirements.txt # already installed in travisnkento:0.0.5
# RUN pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
# RUN pip install mysql-connector-python==8.0.32
RUN pip install bokeh==3.1.0

# I do not want any of my local files inside. 
# I only want the files from the base image
# RUN apt-get install curl wget

ENV SPARK_VERSION=3.3.2 \
HADOOP_VERSION=3 \
JAVA_VERSION=11

# SET JAVA ENV VARIABLES
ENV JAVA_HOME="/home/jdk-${JAVA_VERSION}.0.2"
ENV PATH="${JAVA_HOME}/bin/:${PATH}"

# SET SPARK ENV VARIABLES
ENV SPARK_HOME="/home/spark"
ENV PATH="${SPARK_HOME}/bin/:${PATH}"

# SET PYSPARK VARIABLES
ENV PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

# docker run -it -v /Users/michaelscott/bd4h/project/streaming-env/time-series-kafka-demo:/mount/ --name torch-cont btctothemoonz/travisnkento:0.0.5 
# docker start torch-cont