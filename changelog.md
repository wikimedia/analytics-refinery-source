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
