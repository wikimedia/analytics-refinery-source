## v0.0.92
* Fix wrongly getting the yarn user name in DataFrameToHive
* Fix transform function for NULL values and for dataframes without the webHost column

## v0.0.91
* Update CirrusRequestDeser.java to use new schema of mediawiki/cirrussearch/request event
* Add refine transform function to filter our non-wiki hostnames
* Allow for plus signs in the article titles in the PageviewDefinition
* Reduce the size limit of user agent strings in the UAParser

## v0.0.90
* Fix javax.mail dependency conflict introduced by including json-schema-validator
* Improve CamusPartitionChecker error output

## v0.0.89
* Fix wikidata-coeditor job after MWH-refactor
* ClickstreamBuilder: Decode refferer url to utf-8
* Fix EventLoggingSchemaLoader to properly set useragent is_bot and is_mediawiki fields as booleans
* Fix EventLoggingSchemaLoader to not include depcrecated `timestamp` in capsule schema
* RefineTarget - allow missing required fields when reading textual (e.g. JSON) data using JSONSchemas.
* Filter out 15.wikipedia.org and query.wikidata.org from pageview definition

## v0.0.88
* Fix mediawiki_page_history userId and anonymous
* Fix mediawiki_history_reduced checker
* Fix mediawiki-history user event join

## v0.0.87
* Add EventSparkSchemaLoader support to Refine
* Add jsonschema loader and spark converter classes
* Adapt EventLogging/WhiteListSanitization to new way of storing
* Add change_tags and revision_deleted_parts to mediawiki history
* Fix EventLogging schema URI to include format=json
* Reject invalid page titles from pageview dumps
* Correct names in mediawiki-history sql package
* Update mw user-history timestamps
* Fix mediawiki-history-checker after field renamed
* Fix null-timestamps in checker
* Fix mediawiki-user-history writing filter
* Update mediawiki-history user bot fields

## v0.0.86
-- skipped due to deployment complications https://phabricator.wikimedia.org/T221466 --

## v0.0.85
* Update big spark job settings following advices from
  https://towardsdatascience.com/how-does-facebook-tune-apache-spark-for-large-scale-workloads-3238ddda0830
* Update graphframes to 0.7.0 in refinery-spark

## v0.0.84
* Update mediawiki-history comment and actor joins
* Update mediawiki-history joining to new actor and comment tables

## v0.0.83
* Add --ignore_done_flag option to Refine
* Add wikitech to pageview definition
* HiveExtensions field name normalize now replaces bad SQL characters with
  "_", not just hyphens.
* Add new Cloud VPS ip addresses to network origin UDF
* Correct typo in refinery-core for Maxmind, getNetworkOrigin and IpUtil
* Allow for custom transforms in DataFrameToDruid

## v0.0.82
* Update hadoop, hive and spark dependency versions
* Fix field name casing bug in DataFrame .convertToSchema
  https://phabricator.wikimedia.org/T211833

## v0.0.81
* Use "SORT BY" instead of "ORDER BY" in mediawiki_history_checker job
* Correctly pass input_path_regex to Refine from EventLoggingSanitization
* HiveExtensions schema merge now better support schema changes of complex
  Array element and Map value types.
  https://phabricator.wikimedia.org/T210465
* HiveExtensions findIncompatibleFields was unused and is removed.
* Upgrade profig lib to 2.3.3 after bug fix upstream
* Upgrade spark-avro to 4.0.0 to match new spark versions

## v0.0.80
* Update DataFrameToHive and PartitionedDataFrame to support
  dynamic partitioning and correct some bugs
* Add WebrequestSubsetPartitioner spark job actually launching
  a job partitioning webrequest using DataFrameToHive and a
  transform function

## v0.0.79
* Upgrade camus-wmf dependency to camus-wmf9
* Fix bug in EventLoggingToDruid, add time measures as dimensions

## v0.0.78
* Rename start_date and end_date to since until in EventLoggingToDruid.scala

## v0.0.77
* Add spark job converting mediawiki XML-dumps to parquet
* Default value of hive_server_url updated in Refine.scala job
* Refactor EventLoggingToDruid to use whitelists and ConfigHelper

## v0.0.76
* Refine Config removes some potential dangerous defaults, forcing users to set them
* EventLoggingToDruid now can bucket time measures into ingestable dimensions

## v0.0.75
* Refine and EventloggingSanitization jobs now use ConfigHelper instead of scopt

## v0.0.74
* Add --table-whitelist flag to EventLoggingSanitization job
* Add ConfigHelper to assist in configuring scala jobs with properties files
  and CLI overrides
* RefineMonitor now uses ConfigHelper instead of scopt

## v0.0.73
* Add usability, advisory and strategy wikimedia sites to pageview definition

## v0.0.72
* Correct MediawikiHistoryChecker for reduced

## v0.0.71
* Update MediawikiHistoryChecker adding reduced
* Add MediawikiHistoryChecker spark job
* Update mediawiki-user-history empty-registration
  Drop user-events for users having no registration date
  (i.e. no edit activity nor registration date in DB)
* Correct mediawiki-history user registration date
  Use MIN(DB-registration-date, first-edit-date)
  instead of COALESCE(DB-registration-date, first-edit-date)

## v0.0.70
* Fix for WhitelistSanitization.scala, allowing null values for struct fields

## v0.0.69
* Fix for CamusPartitionChecker to only send email if errors are encountered

## v0.0.68
* Fix case insensibility for MapMaskNodes in WhitelistSanitization
* Add ability to salt and hash to eventlogging sanitization
* Add --hive-server-url flag to Refine job
* CamusPartitionChecker can send error email reports and override Camus
  properties from System properties.

## v0.0.67
* Add foundation.wikimedia to pageviews
* Track number of editors from Wikipedia who also edit on Wikidata over time
* Update user-history job from username to userText
* Add inline comments to WhitelistSanitization

## v0.0.66
* Add a length limit to webrequest user-agent parsing
* Allow partial whitelisting of map fields in Whitelist sanitization

## v0.0.65
* Update mediawiki-history statistics for better names and more consistent probing
* Fix RefineTarget.inferInputFormat filtering out fiels starting with _

## v0.0.64
* Update regular expressions used to parse User Agent Strings
* Add PartitionedDataFrame to Spark refine job
* Fix bug when merging partition fields in WhitelistSanitization.scala
* Update pageview regex to accept more characters (previously restricted to 2)

## v0.0.63
* Make mediawiki-history statistics generation optional
* Modify output defaults for EventLoggingSanitization
* Correct default EL whitelist path in EventLoggingSanitization
* Correct mediawiki-history job bugs and add unittest
* Add defaults section to WhitelistSanitization
* Identify new search engines and refactor Referer parsing code

## v0.0.62
* Fix MediawikiHistory OOM issue in driver
* Update MediawikiHistory for another performance optimization
* Rename SparkSQLHiveExtensions to just HiveExtensions
* Include applicationId in Refine email failure report
* DataFrameToHive - Use df.take(1).isEmpty rather than exception
* RefineTarget - Use Hadoop FS to infer input format rather than Spark
* DataFrameToHive - Use DataFrame .write.parquet instead of .insertInto
* Correct wikidata-articleplaceholder job SQL RLIKE expression

## v0.0.61
* Fix sys.exit bug in Refine
* Fix LZ4 version bug with maven exclusion in refinery-spark and refinery-job

## v0.0.60
* Big refactor of scala and spark code
** add refinery-spark module for spark oriented libs
** Move non-spark dependent code to refinery-core
* Tweak Mediawiki-history job for performance (mostly partitioning)
* Update Mediawiki-history job to use accumulator to gather stats
* Add Hive JDBC connection to Refine for it to work with Spark 2
* Update spark code to use Spark 2.3.0
* Add new wikidata and pageview tags to webrequest
* Update Refine to use SQL-casting instead of row-conversion

## v0.0.59
* JsonRefine has been made data source agnostic, and now lives in a
  refine module in refinery-job.  The Spark job is now just called 'Refine'.
* Add Whitelist Sanitization code and an EventLogging specific job using it
* Add some handling to Refine for cast-able types, e.g. String -> Long, if possible.
* Added RefineMonitor job to alert if Refine targets are not present.

## v0.0.58
* Refactor geo-coding function and add ISP
* Update camus part checker topic name normalization
* Update RefineTarget inputBasePath matches
* Add GetMediawikiTimestampUDF to refinery-hive
* Factor out RefineTarget from JsonRefine for use with other jobs
* Add configurable transform function to JSONRefine
* Fix JsonRefine so that it respects --until flag
* Clean refinery-job from BannerImpressionStream job
* Add core class and job to import EL hive tables to Druid

## v0.0.57
* Add new package refinery-job-spark-2.1
* Add spark-streaming job for banner-activity

## v0.0.56
* JsonRefine improvements:
** Use _REFINE_FAILED flag to indicate previous failure, so we don't
   re-refine the same bad data over and over again
** Don't fail the entire partition refinement if Spark can resolve
   (and possibly throw out) records with non-critical type changes.
   I.e. don't throw the entire hour away if just a couple records have floats
   instead of ints.  See: https://phabricator.wikimedia.org/T182000

## v0.0.55
* something/something_latest fields change to something_historical/something
* UDF for extracting primary full-text search request
* Fix Clickstream job
* Change Cassandra loader to local quorum write
* Add Mediawiki API to RestbaseMetrics
* Fix mediawiki history reconstruction

## v0.0.54
* refinery-core now builds scala.
* Add JsonRefine job

## v0.0.53
* Correct field names in mediawiki-history spark job (time since previous revision)
* Add PhantomJS to the bot_flagging regex
* Correct mobile-apps-sessions spark job (filter out `ts is null`)

## v0.0.52
* Add Clickstream builder spark job to refinery-job
* Move GraphiteClient from refinery-core to refinery-job

## v0.0.51
* Correct bug in host normalization function - make new field be at the end of the struct

## v0.0.50
* Update host normalization function to return project_family in addition to project_class
  (with same value) in preparation to remove (at some point) the project_class field.

## v0.0.49
* Add webrequest tagging (UDF to tag requests) https://phabricator.wikimedia.org/T164021
  * Tagger can return several tags (same task as above)
  * Correct null pointer exception (same task as above)
* Add webrequest tagger for Wikidata Query Service https://phabricator.wikimedia.org/T169798

## v0.0.48
* Update mediawiki_history job with JDBC compliant timestamps and per-user and per-page
  new fields (revision-count and time-from-previous-revision)
* Removed unused and deprecated ClientIpUDF. See also https://phabricator.wikimedia.org/T118557
* Mark Legacy Pageview code as deprecated.

## v0.0.47
* Update tests and their dependencies to make them work on Mac and for any user.

## v0.0.46
* Add small cache to avoid repeating normalization in Webrequest.normalizeHost
* Refactor PageviewDefinition to add RedirectToPageviewUDF
* Add support for both cs and cz as Czech Wiki Abbreviations to StemmerUDF

## v0.0.45
* Remove is_productive and update time to revert from MediaWiki history denormalizer
* Add revision_seconds_to_identity_revert to MediaWiki history denormalizer
* Use hive query instead of parsing non existent sampled TSV files for guard settings

## v0.0.44
* Update mediawiki history jobs to overwrite result folders

## v0.0.43
* Add mediawiki history spark jobs to refinery-job
* Add spark job to aggregate historical projectviews
* Do not filter test[2].wikipedia.org from pageviews

## v0.0.42
* Upgrade hadoop, hive and spark version after CDH upgrade.
  Hadoop and hive just have very minor upgrades, spark has a
  more import one (from 1.5.0 to 1.6.0.)
* Change the three spark jobs in refinery-job to have them
  working with the new installation (this new installation
  has a bug preventing using HiveContext in oozie).

## v0.0.41
* Update pageview definition to remove previews https://phabricator.wikimedia.org/T156628
* Add spark streaming job for banner impressions https://phabricator.wikimedia.org/T155141
* Add DSXS (self-identified bot) to bot regex https://phabricator.wikimedia.org/T157528

## v0.0.40
* Add comment to action=edit filter in pageview definition: https://phabricator.wikimedia.org/T156629

## v0.0.39
* Standarize UDF Naming:  https://phabricator.wikimedia.org/T120131
* Lucene Stemmer UDF https://phabricator.wikimedia.org/T148811

## v0.0.38
* WikidataArticlePlaceholderMetrics also send search referral data https://phabricator.wikimedia.org/T142955
* Adding self-identified bot to bot regex https://phabricator.wikimedia.org/T150990

## v0.0.37
* Modify user agent regexes to identify iOS pageviews on PageviewDefinition
https://phabricator.wikimedia.org/T148663

## v0.0.36
* Count pageviews for more wikis, https://phabricator.wikimedia.org/T130249

## v0.0.35
* Classify DuckDuckGo as a search engine
* Make camus paritition checker continue checking other topics
  if it encounters errors

## v0.0.34
* Update maven jar building in refinery (refinery-core is not uber anymore)
* Create WikidataSpecialEntityDataMetrics
* Fix WikidataArticlePlaceholderMetrics class doc

## v0.0.33
* Correct WikidataArticlePlaceholderMetrics

## v0.0.32
* Add WikidataArticlePlaceholderMetrics

## v0.0.31
* Fixes Prefix API request detection
* Refactor pageview definition for mobile apps
* Remove IsAppPageview UDF

## v0.0.30
* Add pageview definition special case for iOs App
* Correct CqlRecordWriter in cassandra module
* Evaluate Pageview tagging only for apps requests

## v0.0.29 [SKIPPED]

## v0.0.28
* Update mediawiki/event-schemas submodule to include information about
  search results in CirrusSearchRequestSet
* Drop support for message without rev id in avro decoders and make
  latestRev mandatory
* Upgrade to latest UA-Parser version
* Update mediawiki/event-schemas submodule to include 3dd6ee3 "Rename
  ApiRequest to ApiAction".
* Google Search Engine referer detection bug fix
* Upgrade camus-wmf dependency to camus-wmf7
* Requests that come tagged with pageview=1 in x-analytics header
  are considered pageviews

## v0.0.27
* Upgrade CDH dependencies to 5.5.2
* Implement the Wikimedia User Agent policy in setting agent type.
* Remove WikimediaBot tagging.
* Add ApiAction avro schema.
* Add functions for categorizing search queries.
* Update CamusPartitionChecker not to hard-failing on errors.
* Add CamusPartitionChecker the possibility to rewind to last N runs
  instead of just one.
* Update AppSession Metrics with explicit typing and sorting improvement
* Ensure that the schema_repo git submodules are available before packaging

## v0.0.26
* REALLY remove mobile partition use.  This was reverted and never
  deployed in 0.0.25
* Add split-by-os argument to AppSessionMetrics job

## v0.0.25
* Change/remove mobile partition use
* Add Functions for identifying search engines as referers
* Update avro schemas to use event-schema repo as submodule

## v0.0.24
* Implement ArraySum UDF
* Clean refinery-camus from unnecessary avro files
* Add UDF that turns a country code into a name

## v0.0.23
* Expand the prohibited uri_paths in the Pageview definition
* Make maven include avro schema in refinery-camus jar
* Add a Hive UDF for network origin updating existing IP code
* Correct CamusPartitionChecker unit test
* Add refinery-cassandra module, containing the necessary code
  for loading separated value data from hadoop to cassandra
* Update refinery-camus adding support for avro messages
* Add CirrusSearchRequestSet avro schema to refinery camus
* Update webrequest with an LRUCache to prevent recomputing
  agentType for recurrent user agents values

## v0.0.22
* Correct CamusPartitionChecker bug.
* Expand the prohibited URI paths in the pageview definition.

## v0.0.21
* Update CirrusSearchRequestSet avro schema.
* Update CamusPartitionChecker with more parameters.

## v0.0.20
* Add refinery-camus module
* Add Camus decoders and schema registry to import Mediawiki Avro Binary data
  into Hadoop
* Add camus helper functions and job that reads camus offset files to check
  if an import is finished or not.

## v0.0.19
* Update regexp filtering bots and rename Webrequest.isCrawler to
  Webrequest.isSpider for consitency.
* Update ua-parser dependency version to a more recent one.
* Update PageviewDefinition so that if x-analytics header includes tag preview
  the request should not be counted as pageview.

## v0.0.18
* Add scala GraphiteClient in core package.
* Add spark job computing restbase metrics and sending them to graphite in
  job package.

## v0.0.17
Correct bug in PageviewDefinition removing arbcom-*.wikipedia.org

## v0.0.16
* Correct bug in PageviewDefinition removing outreach.wikimedia.org
  and donate.wikipedia.org as pageview hosts.
* Correct bug in page_title extraction, ensuring spaces are always
  converted into underscores.

## v0.0.15
* Correct bug in PageviewDefinition ensuring correct hosts only can be flagged
  as pageviews.

## v0.0.14
* Add Spark mobile_apps sessions statistics job.

## v0.0.13
* Fix bug in Webrequest.normalizeHost when uri_host is empty string.

## v0.0.12
* wmf_app_version field now in map returned by UAParserUDF.
* Added GetPageviewInfoUDF that returns a map of information about Pageviews
  as defined by PageviewDefinition.
* Added SearchRequestUDF for classifying search requests.
* Added HostNormalizerUDF to normalize uri_host fields to regular WMF URIs formats.
  This returns includes a nice map of normalized host info.

## v0.0.11
* Build against CDH 5.4.0 packages.

## v0.0.10
* Maven now builds non-uber jars by having hadoop and hive in provided scope.
  It also takes advantage of properties to propagate version numbers.
* PageView Class has a function to extract project from uri.
  Bugs have been corrected on how to handle mobile uri.
* Referer classification now outputs a string instead of a map.

## v0.0.9
* Generic functions used in multiple classes now live in a single "utilities" class.
* Pageview and LegacyPageview have been renamed to PageviewDefinition and
  LegacyPageviewDefinition, respectively.  These also should now use the
  singleton design pattern, rather than employing static methods everywhere.
* renames isAppRequest to isAppPageview (since that's what it does) and exposes
  publicly in a new UDF.
* UAParser usage is now wrapped in a class in refinery-core.

## v0.0.8
* Stop counting edit attempts as pageviews
* Start counting www.wikidata.org hits
* Start counting www.mediawiki.org hits
* Consistently count search attempts
* Make custom file ending optional for thumbnails in MediaFileUrlParser
* Fail less hard for misrepresented urls in MediaFileUrlParser
* Ban dash from hex digits in MediaFileUrlParser
* Add basic guard framework
* Add guard for MediaFileUrlParser

## v0.0.7
* Add Referer classifier
* Add parser for media file urls
* Fix some NPEs around GeocodeDataUDF

## v0.0.6
* Add custom percent en-/decoders to ease URL normalization.
* Add IpUtil class and ClientIP UDF to extract request IP given IP address and X-Forwarded-For.

## v0.0.5
* For geocoding, allow to specify the MaxMind databases that should get used.

## v0.0.4
* Pageview definition counts 304s.
* refinery-core now contains a LegacyPageview class with which to classify legacy pageviews from webrequest data
* refinery-hive includes IsLegacyPageviewUDF to use legacy Pageview classification logic in hive. Also UDFs to get webrequest's access method, extract values from the X-Analytics header and determine whether or not the request came from a crawler, and geocoding UDFs got added.

## v0.0.3
* refinery-core now contains a Pageview class with which to classify pageviews from webrequest data
* refinery-hive includes IsPageviewUDF to use Pageview classification logic in hive
