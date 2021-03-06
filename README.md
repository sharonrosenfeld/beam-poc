this project is initially forked from apach-beam starter. it contains a running pipeline deomnstrating several beam functions of maps and reduces ( groupBy ) etc.. 

# Apache Beam Starter

This is a starter repo for Apache Beam. You can read the blog post [A quick demo of Apache Beam with Docker](https://medium.com/@ecesena/a-quick-demo-of-apache-beam-with-docker-da98b99a502a).

It currently contains a [WordCount pipeline](https://github.com/ecesena/beam-starter/blob/master/src/main/java/com/dataradiant/beam/examples/WordCount.java) for the Apache Flink runner, and it's used in this [Docker image with Apache Beam + Flink](https://github.com/ecesena/docker-beam-flink).

More interesting examples coming soon. In the meantime, this can be used as a starter repo to create new Beam pipelines, check out the pom.xml and change it as needed.

If you start a project forking this repo, [I'd love to hear about it](http://twitter.com/emacesena)!

## Build

Requirements: Java 1.7+, Maven (tested on 3.3.9)

```
git clone https://github.com/ecesena/beam-starter
cd beam-starter
mvn clean package
```

## Open Source

 - Based on instructions from https://github.com/apache/incubator-beam/tree/master/runners/flink
 - Apache Beam: https://beam.incubator.apache.org
 - Apache Flink: https://flink.apache.org
 - Docker image with Apache Beam + Flink: https://github.com/ecesena/docker-beam-flink
 - Blog: [A quick demo of Apache Beam with Docker](https://medium.com/@ecesena/a-quick-demo-of-apache-beam-with-docker-da98b99a502a)
