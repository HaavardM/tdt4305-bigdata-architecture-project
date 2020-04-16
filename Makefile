

generate_jar:
	sbt package
	mv target/scala-2.11/tdt4305-bigdata-architecture-project_2.11-0.1.jar spark_submit_me.jar

get_source:
	cp src/main/scala/sentiment_analysis.scala sentiment_analysis.scala

.PHONY: generate_jar get_source

all: generate_jar get_source
	mkdir -p delivery/data
	touch delivery/data/ADD_DATASETS_HERE
	mv spark_submit_me.jar delivery/spark_submit_me.jar
	mv sentiment_analysis.scala delivery/sentiment_analysis.scala
	cp README.md delivery/README.md
	cp *.txt delivery