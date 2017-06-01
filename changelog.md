## v0.0.46

* Add small cache to avoid repeating normalization of hosts
* Refactor PageviewDefinition to add RedirectToPageviewUDF
* Add support for both cs and cz as Wiki Abbreviations for Czech

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
