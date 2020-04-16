# tdt4305-bigdata-architecture-project
Project for TDT4305 Big Data Architecture

The data files are not included here, download all datasets into the `data` folder.

We've used JDK 8!

# Part 1
## How to run

For task 1,2,3 and 4 (RDD tasks) call

```
sbt "runMain part1rdd"
```

For task 5 call
```
sbt "runMain part1df"
```

# Part 2

## Using included jar
Our code has been packaged into a jar which is runnable using spark:
```
spark-submit spark_submit_me.jar
```
The jar depends on the AFINN-111.txt and stopwords.txt files, as well as the datasets in the data folder (not included). 

## Using SBT
If the scala build system `SBT` is installed on your computer you can run our code using:

```
sbt "runMain sentiment_analysis"
```
Spark will automatically be downloaded by sbt. 
