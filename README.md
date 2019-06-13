# Expert Recomendation on Stack Overflow

## Environment Setup


### AWS Setup

#### VPC

#### Subnet

#### IAM

#### Security Groups

#### Launch EC2 instances

#### passwordless SSH

### Saprk
#### Install Java and Set Java Path
You should install Java8 on each instance used for Spark.

```
sudo apt update
sudo apt install openjdk-8-jdk
java -version # check version of Java you installed
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 # use echo $JAVA_HOME to check $JAVA_HOME
export PATH=$PATH:$JAVA_HOME/bin # echo $PATH
```

## Data
data source: [Stack Enchange Data Dump](https://archive.org/details/stackexchange)

We could use ```wget``` to download the data from web to our instance. We then need to unzip the file on EC2 using ```p7zip-full```.
```
sudo apt-get instll p7zip-full
7z x filename
```

### Upload data from EC2 to S3
```
aws configuration
aws s3 cp local_file s3://stackoverflow-full-insight/
```