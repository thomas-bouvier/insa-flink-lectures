# insa-flink-lectures

# Set up the environment

Exemples are intended to be run with Flink >=1.18.

Flink runs on all UNIX-like environments, i.e. Linux, macOS, and Cygwin (for Windows). You need to have Java 17 installed. To check the Java version installed, type in your terminal:

```
java -version
```

This repository leverages [Maven](https://maven.apache.org/install.html) >3.8 to install dependencies:

```
mvn -v
```

The result should look similar to:

```
Apache Maven 3.9.6 (bc0240f3c744dd6b6ec2920b3cd08dcc295161ae)
Maven home: /home/tbouvier/.sdkman/candidates/maven/current
Java version: 17.0.9, vendor: Eclipse Adoptium, runtime: /home/tbouvier/.sdkman/candidates/java/17.0.9-tem
Default locale: en_GB, platform encoding: UTF-8
OS name: "linux", version: "6.6.7-100.fc38.x86_64", arch: "amd64", family: "unix"
```

If you get any issue, [SDKMAN](https://sdkman.io/usage) is useful to manage your environment.

```
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

sdk install java 17.0.9-tem
sdk install maven
sdk install flink
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
mvn install exec:java -Dmain.class="io.thomas.ExampleTumblingJoin" -Dexec.args="--input1 country-ids-stream.txt --input2 people-ids-stream.txt" -q
```

Please use absolute paths for input files.

# Data producers

Windowing example require some data to be produced.

```
cd lectures/cm4
mvn install exec:java -Dmain.class="io.thomas.producers.DataProducer" -q
```

Run your example afterwards.

# Misc

To write text data on a socket, which may be ingested by Flink:

```
nc -lk 9090
```

Check whether socket 9090 is open:

```
lsof -i -P -n | grep 9090
```
