# Mediawiki History Reconstruction in Spark -- Manual

History reconstruction process has three main steps:
* Users history reconstruction
* Pages history reconstruction
* Revisions augmentation and denormalization

users and pages history reconstruction have no dependencies
among them and can be run in any order, and revisions
augmentation and denormalization depends on both first ones.


## Prerequisites


### Mediawiki projects' databases

We use *sqoop* to generate extracts of the per-project-databases
tables in *avro* format. We also take advantage of Spark ability
to read hive-style partitions like `/path/field_name=value_name/files`.
Finally we have a script converting MediaWiki SiteMatrix information
into a CSV, providing language-localized namespace headers for our projects.

Expected directory layout for tables is:
~~~~
hdfs:///any/base/path/tables/archive
hdfs:///any/base/path/tables/logging
hdfs:///any/base/path/tables/page
hdfs:///any/base/path/tables/revision
hdfs:///any/base/path/tables/user
hdfs:///any/base/path/tables/user_groups
~~~~
and within each table directory:
~~~~
hdfs:///any/base/path/tables/archive/wiki_db=project1/files.avro
 ...
hdfs:///any/base/path/tables/archive/wiki_db=projectN/files.avro
~~~~
Lastly project namespace directory should be:
~~~~
hdfs:///any/base/path/project_namespace_map/any_files.csv
~~~~


### A spark cluster with enough resources!

The *full database extract* (needed tables from every project)
is taking more than **230Gb** in our hdfs storage, among which revision data
is about 173Gb, logging 42Gb, page 10Gb and user 5Gb.

While the first two steps of users and pages history reconstruction can be
done with a small-ish amount of RAM per spark executor (2Gb for 1 core),
the revision augmentation and denormalization step doesn't fit in less
than 8Gb per core. Also, growing the number of partitions doesn't help
here since the problem comes from skewed grouped data (which needs to be
processed on on single executor).


## Execution

**Launch examples use joal settings**

**Our CDH version doesn't include the Spark versions we need**

Command line help:
~~~~
/home/joal/code/spark-1.6.3-bin-hadoop2.6/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--class org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiHistoryRunner \
/home/joal/code/refinery-source/refinery-job/target/refinery-job-0.0.39-SNAPSHOT.jar \
--help


Mediawiki History Runner
Usage: [options]

Builds user history, page history, and join them with revision
history into a fully denormalized parquet table by default.
You can specify which sub-jobs you wish to run if you think
that `you gotta keep'em separated`.
  --help
        Prints this usage text
  -i <path> | --mediawiki-base-path <path>
        Base path to mediawiki extracted data on hadoop.
        Defaults to hdfs://analytics-hadoop/wmf/data/raw/mediawiki
  -o <path> | --output-base-path <path>
        Path to output directory base.
        Defaults to hdfs://analytics-hadoop/wmf/data/wmf/mediawiki
  -w <wiki_db_1>,<wiki_db_2>... | --wikis <wiki_db_1>,<wiki_db_2>...
        wiki dbs to compute
  -t <path> | --temporary-path <path>
        Path to use as checkpoint directory for Spark (temporary data).
        Defaults to hdfs://analytics-hadoop/tmp/mediawiki/history/checkpoints
  -n <value> | --num-partitions <value>
        Number of partitions to split users and pages datasets by.
        Revisions dataset is split by 8 * num-partitions. Defaults to 64
  --debug
        debug mode -- spark logs added to applicative logs (VERY verbose)
  --users-history
        Run specific step(s) -- Users History
  --pages-history
        Run specific step(s) -- Pages History
  --revisions-denormalize
        Run specific step(s) -- Revisions & denormalization

~~~~

