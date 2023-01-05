# insa-flink-lectures

# Set up the environment

Flink runs on all UNIX-like environments, i.e. Linux, Mac OS X, and Cygwin (for Windows). You need to have Java 11 installed. To check the Java version installed, type in your terminal:

```
java -version
```

This repository leverages [Maven](https://maven.apache.org/install.html) >3.8 to install dependencies:

```
mvn -v
```

The result should look similar to:

```
Apache Maven 3.8.4 (Red Hat 3.8.4-3)
Maven home: /usr/share/maven
Java version: 11.0.8, vendor: N/A, runtime: /home/tbouvier/.sdkman/candidates/java/11.0.8-open
Default locale: en_GB, platform encoding: UTF-8
OS name: "linux", version: "6.0.16-200.fc36.x86_64", arch: "amd64", family: "unix"
```

# Execute an example

```
git clone https://github.com/thomas-bouvier/insa-flink-lectures.git
```

Place yourself in a directory containing a `pom.xml` file:

```
cd lectures/cm3
```

Install the dependencies and compile the example as follows:

```
mvn install exec:java -Dmain.class="io.thomas.WordCount" -q
```

If the example you want to run requires input parameters:

```
mvn install exec:java -Dmain.class="io.thomas.ExampleJoin" -Dexec.args="--input1 country-ids-stream.txt --input2 people-ids-stream.txt" -q
```

Please use absolute paths for input files.