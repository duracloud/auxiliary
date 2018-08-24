Copy Content Tool
==================

Provides a simple way to copy content from one set of spaces to another set of spaces. 
If you do not provide a list of spaces in a separate file,  all spaces will be copied 
with the following algorithm: 

If the space matches the regex "^(.*)-(open|campus|closed)$",  content will be copied from the source space
to a new space (open|campus|closed) with a contenId of "(.*)/${source-content-id}. 

So for example, given the space  "space1234-open" and a content ID of my-mp3-file.mp3, the tool
will create a space name "open" if it doesn't already exist and copy the content item  into a new content item with ID
"space1234/my-mp3-file.mp3". 

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
java -jar copy-content-tool-<version>-driver.jar
```
This will display help text that indicates the necessary parameters.

# More Information
Further documentation for this tool can be found [on the DuraCloud wiki](https://wiki.duraspace.org/display/DURACLOUDDOC/Auxiliary+Tools)
