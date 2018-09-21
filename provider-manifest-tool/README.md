Provider Manifest Tool
==================

Used to generate a manifest of content directly from a storage provider. Can be used to compare the with the DuraCloud manifest.

# Dependencies
This tool has the following dependencies
* The DuraCloud service
* Java 8+

# Building
Once cloned, this tool can be built using:
```
mvn install
```

# Running
This tool builds into an executable JAR file, which can be run using the following command:
```
java -jar provider-manifest-tool-<version>-driver.jar
```
This will display help text that indicates the necessary parameters.

# More Information
Further documentation for this tool can be found [on the DuraCloud wiki](https://wiki.duraspace.org/display/DURACLOUDDOC/Auxiliary+Tools)
