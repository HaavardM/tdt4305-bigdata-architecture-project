

generate_jar:
	sbt package
	mv target/scala-2.11/tdt4305-bigdata-architecture-project_2.11-0.1.jar spark_submit_me.jar

get_source:
	cp src/main/scala/part2.scala sentiment_analysis.scala

.PHONY: generate_jar get_source

all: generate_jar get_source