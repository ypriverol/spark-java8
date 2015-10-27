# Java 8 and Spark Learning tutorials

This is a collection of [Java 8](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html) and [Apache Spark](http://spark.apache.org/) examples and concepts,
from basic to advanced. It explain basic concepts introduced by Java 8 and how you can merge them with Apache Spark.

The current tutorial or set of examples provide a way of understand Spark 2.0 in details but also to get familiar with
Java 8 and it new features like lambda, Stream and reaction programming.

## Why Java 8

Java 8 is the latest version of Java which includes two major changes: Lambda expressions and Streams. Java 8 is a revolutionary release of the world’s #1 development platform.
It includes a huge upgrade to the Java programming model and a coordinated evolution of the JVM, Java language, and libraries. Java 8 includes features for productivity,
ease of use, improved polyglot programming, security and improved performance. Welcome to the latest iteration of the largest, open, standards-based, community-driven platform.

 1- Lambda Expressions, a new language feature, has been introduced in this release. They enable you to treat functionality as a method argument, or code as data.
    Lambda expressions let you express instances of single-method interfaces (referred to as functional interfaces) more compactly.

 2- Classes in the new java.util.stream package provide a Stream API to support functional-style operations on streams of elements. The Stream API is integrated into the Collections API,
    which enables bulk operations on collections, such as sequential or parallel map-reduce transformations including performance improvement for HashMaps with Key Collisions.

## Why Spark

[Apache Spark™](http://spark.apache.org/) is a fast and general engine for large-scale data processing. Apache Spark is a fast and general-purpose cluster computing system. It provides high-level APIs in Java,
Scala, Python and R, and an optimized engine that supports general execution graphs. It also supports a rich set of higher-level tools including Spark SQL for SQL and structured data processing, MLlib for machine
learning, GraphX for graph processing, and Spark Streaming.

## Instructions

A good way of using these examples is by first cloning the repo, and then
starting your own [Spark Java 8](http://github.con/ypriverol/spark-java8).

### Installing Java 8 and Spark

Java 8 can be download [here](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html). After the installation
you need to be sure that the version you are using is java 8, you can check that by running:

```bach
java -version
```

In order to setup Spark locally in you machine you should download the spark version from [here](http://spark.apache.org/downloads.html). Then you should follow the next steps:

```bach
> tar zxvf spark-xxx.tgz
> cd spark-xxx
> build/mvn -DskipTests clean package
```

After the compilation and before running your first example you should add to your profile the [SPARK MASTER Variable](http://spark.apache.org/docs/latest/spark-standalone.html):

```bach
 > export SPARK_LOCAL_IP=127.0.0.1
```
To be sure that you spark is installed properly in your machine you can run the first example from spark:

```bach
> ./bin/run-example SparkPi
```

## Datasets

Some of the datasets we will use in this learning tutorial are:
  - Tweets Archive from [@ypriverol](https://twitter.com/ypriverol) is used in the word count
  - We will be using datasets from the [KDD Cup 1999](http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html). The results
    of this competition can be found [here](http://cseweb.ucsd.edu/~elkan/clresults.html).

## References

The reference book for these and other Spark related topics is:

- *Learning Spark* by Holden Karau, Andy Konwinski, Patrick Wendell, and Matei Zaharia.

## Examples

The following examples can be examined individually, although there is a more or less linear 'story' when followed in sequence. By using different datasets they try to solve a related set of tasks with it.

### [RDD Basic Examples](https://github.com/ypriverol/spark-java8/wiki/2-RDD-Basic-Examples)

Here a list of the most basic examples in Spark-Java8 and definition of the most basic concepts in Spark.

 1- [SparkWordCount](https://github.com/ypriverol/spark-java8/wiki/2.1--Spark-Word-Count): About How to create a simple JavaRDD in Spark.
 
 2- [MaptoDouble](https://github.com/ypriverol/spark-java8/wiki/2.2-Map-To-Double): How to generate general statistics about an RDD in Spark
 
 3- [SparkAverage](https://github.com/ypriverol/spark-java8/wiki/2.3-Spark-Average): How to compute the average of a set of numbers in Spark.

### [RDD Sampling Examples](https://github.com/ypriverol/spark-java8/wiki/3-Spark-Sampling-Examples)

 1- [SparkSampling](https://github.com/ypriverol/spark-java8/wiki/3.1-SparkSampling): Basic Spark Sampling using functions sample and takesample.  
