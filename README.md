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
starting your own [Spark Java 8](http://github.con/ypriverol/spark-java8. For example, if we have a *standalone* Spark installation
running in our `localhost` with a maximum of 6Gb per node assigned:

    MASTER="spark://127.0.0.1:7077" SPARK_EXECUTOR_MEMORY="6G" ~/spark-1.5.0-bin-hadoop2.6/bin/pyspark

Notice that the path to the `pyspark` command will depend on your specific
installation. So as requirement, you need to have
[Spark installed](https://spark.apache.org/docs/latest/index.html) in
the same machine you are going to start the `IPython notebook` server.

For more Spark options see [here](https://spark.apache.org/docs/latest/spark-standalone.html). In general it works the rule of passing options
described in the form `spark.executor.memory` as `SPARK_EXECUTOR_MEMORY`.

## Datasets

Some of the datasets we will use in this learning tutorial are:
  - Tweets Archive from @ypriverol (https://twitter.com/ypriverol) is used in the word count
  - We will be using datasets from the [KDD Cup 1999](http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html). The results
    of this competition can be found [here](http://cseweb.ucsd.edu/~elkan/clresults.html).

## References

The reference book for these and other Spark related topics is:

- *Learning Spark* by Holden Karau, Andy Konwinski, Patrick Wendell, and Matei Zaharia.

## Examples

The following examples can be examined individually, although there is a more or less linear 'story' when followed in sequence. By using different datasets they try to solve a related set of tasks with it.

### [RDD Basic Examples](https://github.com/ypriverol/spark-java8/wiki/RDD-Basic-Examples)

 1- [SparkWordCount](): About How to create a simple JavaRDD in Spark.


