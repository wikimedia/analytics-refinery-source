# Recipe to recreate test data

Mediawiki Dumper tests needs realistic data.

Here is how `wmf_content_mediawiki_content_history_v1.json.gz` was created.

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

// select interesting examples that vary the XML output
// but also since this data will be public, we need to honor visibility flags
val simpleDF = spark.sql(
    s"""
    SELECT
    page_id,
    page_namespace_id,
    page_title,
    page_redirect_target,
    user_id,
    IF(user_is_visible, user_text, 'redacted') AS user_text,
    user_is_visible,
    revision_id,
    revision_parent_id,
    revision_dt,
    revision_is_minor_edit,
    IF(revision_comment_is_visible, revision_comment, 'redacted') AS revision_comment,
    revision_comment_is_visible,
    IF(revision_content_is_visible, revision_sha1, 'redacted') AS revision_sha1,
    IF(revision_content_is_visible, revision_size, -1L) AS revision_size,
    transform_values(revision_content_slots,
                        (k, v) -> (
                            IF(revision_content_is_visible, v.content_body, 'redacted') AS content_body,
                            v.content_format,
                            v.content_model,
                            IF(revision_content_is_visible, v.content_sha1, 'redacted') AS content_sha1,
                            IF(revision_content_is_visible, v.content_size, -1L) AS content_size,
                            v.origin_rev_id
                        )
    ) AS revision_content_slots,
    revision_content_is_visible,
    wiki_id,
    row_content_update_dt,
    row_visibility_update_dt,
    row_move_update_dt
    FROM wmf_content.mediawiki_content_history_v1
    WHERE ( wiki_id = 'simplewiki' AND page_id in (45046, 279900) )
    OR ( wiki_id = 'commonswiki' AND page_id in (13327093) )
    ORDER BY wiki_id, page_id, revision_id
    """)

simpleDF
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("compression","gzip")
    .json("/tmp/wmf_content_mediawiki_content_history_v1.json.gz")
    
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
