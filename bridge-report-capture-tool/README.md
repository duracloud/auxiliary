Bridge Report Capture Tool
==========================

Provides a simple way to capture a DuraCloud bridge report and store that report in S3.

# Dependencies
This tool has the following dependencies
* The DuraCloud bridge application (running externally)
* Java 8+
* An AWS account, with API credentials. The credentials must provide write access to at least one S3 bucket. 

# Building
Once cloned, this tool can be built using:
```
mvn install
```

# Running
This tool builds into an executable JAR file, which can be run using the following command:
```
java -jar bridge-report-capture-tool-<version>-driver.jar
```
This will display help text that indicates the necessary parameters.

# More Information
Further documentation for this tool can be found [on the DuraCloud wiki](https://wiki.duraspace.org/display/DURACLOUDDOC/Auxiliary+Tools)
