Transcoding Job Generator
=========================

Simple client that will generate a Job in AWS Elastic Transcoder for every file in an S3 bucket. 

# Dependencies
This tool has the following dependencies
* Java 8+
* An AWS account

# Building
Once cloned, this tool can be built using:
```
mvn install
```

# Running
This tool builds into an executable JAR file, which can be run using the following command:
```
java -jar transcoding-job-generator-<version>-driver.jar
```
This will display help text that indicates the necessary parameters.
