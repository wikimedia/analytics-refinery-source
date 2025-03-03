# Recipe to recreate test data

Mediawiki Dumper tests needs realistic data.

Here is how wmf_dumps_wikitext_raw_rc1.json.gz was created.

### 1/ Find a good set of pages
```roomsql
 -- get a couple of pages with a short-ish history and all the
 -- variety we need to test for (on analytics replicas - simplewiki)
 select rev_page
   from revision
            inner join
        actor           on actor_id = rev_actor
  group by rev_page
        -- not too many but not too few edits
 having count(1) between 10 and 20
        -- some suppressed users
    and sum(if(rev_deleted & 4 > 0, 1, 0)) > 0
        -- some suppressed comments
    and sum(if(rev_deleted & 2 > 0, 1, 0)) > 0
        -- some suppressed content
    and sum(if(rev_deleted & 1 > 0, 1, 0)) > 0
        -- some IP editors
    and sum(if(actor_user is null, 1, 0)) > 0
        -- some minor edits
    and sum(if(rev_minor_edit, 1, 0)) > 0
  limit 3
  
+----------+
| rev_page |
+----------+
|    45046 |
|   213660 |
|   279900 |
+----------+
```

### 2/ Build archives on the cluster with spark-shell
```
spark3-shell --master yarn

// TODO: select interesting examples that vary the XML output:
//    * temp contributors
//    etc...
val simpleDF = spark.sql(
    s"""| SELECT * FROM wmf_dumps.wikitext_raw_rc1
        |  WHERE wiki_db = 'simplewiki'
        |    AND page_id in (45046, 279900)
        |""".stripMargin)

simpleDF
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("compression","gzip")
    .json("/tmp/wmf_dumps_wikitext_raw_rc1.json.gz")
    
val namespacesDF = spark.sql(
    s"""| SELECT * FROM milimetric.mediawiki_project_namespace_map
        |  WHERE snapshot = '2023-09'
        |""".stripMargin)

namespacesDF
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("compression","gzip")
    .json("/tmp/wmf_raw_mediawiki_project_namespace_map.json.gz")
```
### 3/ Move data to your local machine

```bash
cd refinery-job/src/test/resources/mediawikidumper/
# this just wrote to hdfs at
# /tmp/wmf_content_mediawiki_content_history_v1.json.gz/part....json.gz
# /tmp/wmf_raw_mediawiki_project_namespace_map.json.gz/part...json.gz
# so adjust accordingly
scp stat1004.eqiad.wmnet:/mnt/hdfs/tmp/...
scp stat1004.eqiad.wmnet:/mnt/hdfs/tmp/...
```

### 4/ Get XML sample from real dumps
``` bash
cd refinery-job/src/test/resources/mediawikidumper/
/bin/python filterDumps.py /path/to/dump.xml 45046,279900 > MediawikiDumperOutputTest.xml
```
