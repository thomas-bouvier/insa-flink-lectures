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

If you get any issue, [SDKMAN](https://sdkman.io/usage) is useful to manage your environment.

```
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 11.0.8-open
sdk install maven
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

# Misc

To write text data on a socket, which may be ingested by Flink:

```
nc -lk 9090
```

Check whether socket 9090 is open:

```
lsof -i -P -n | grep 9090
```

Flink related commands, formerly used by Daniel:

```
/home/student/tp/flink-1.9.0/bin/start-cluster.sh
/home/student/tp/flink-1.9.0/bin/flink run /home/student/tp/eclipse-workspace/wc/target/wc-0.0.1-SNAPSHOT.jar --input file:///home/student/wc-in.txt --output file:///home/student/wc-out.txt
/home/student/tp/flink-1.9.0/bin/flink run -c cm3.WordCountFilter /home/student/tp/eclipse-workspace/wc/target/wc-0.0.1-SNAPSHOT.jar --input file:///home/student/word-stream.txt --output file:///home/student/wc-out-filter.txt
/home/student/tp/flink-1.9.0/bin/stop-cluster.sh
```