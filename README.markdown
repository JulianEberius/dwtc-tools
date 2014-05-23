## DWTC-Tools: A Java Library for working with the Dresden Web Table Corpus

This repository contains Java classes to help you get started using the [Dresden Web Table Corpus](http://wwwdb.inf.tu-dresden.de/misc/edyra/dwtc), a collection of roughly 100M data tables extracted from a publicly available web crawl, the [Common Crawl](http://commoncrawl.org).

It contains code for:

- parsing / deserializing the data to Java objects (package *webreduce.data*)
- parallel (single machine) iteration over the corpus with examples (package *webreduce.iterator*). Since the corpus is only 30GB when compressed, processing it on one machine using these specialized iterators is usually faster than using Hadoop or similar software. Still, a sample Hadoop job is also provided, see below.
- creating a Lucene index over the corpus, including some preprocessing (package *webreduce.indexing*)
- accessing the full text of the originating page of each table directly from Common Crawls S3 when needed (package *webreduce.fulltext*)
- the Hadoop deduplication job that was applied to the raw extract data, which can also serve as a template for other Hadoop jobs (package *webreduce.hadoop*)
- various utilities, e.g., heuristics for typing columns (identifying number columns etc.)

### Getting Started
First, visit the [DWTC page](https://wwwdb.inf.tu-dresden.de/edyra/dwtc) to download the corpus (or any subset of it) using the instructions found there. There is no need to decompress the data, both the iterators provided here and Hadoop can work on the compressed data directly.

The easiest way to build the library is using Maven (or importing into an Maven-compatible IDE):

    mvn install

This will also create a standalone JAR file containing all dependencies in the *target* directory. Then, to run, for example, the bundled example iterator that extracts the first line of each table:

    java -cp target/dwtc-tools-1.0.0-jar-with-dependencies.jar webreduce.iterator.examples.AttributesExtractor ~/dwtc-corpus ~/attributes.csv

The flags used in the example are documented in the code.

Generally, to process the data, either subclass WebreduceIterator as in the provided examples, modify the provided Hadoop job, or work on uncompressed JSON documents using just the class *Dataset* which provides a fromJson() static method for parsing the lines of the corpus.