## v0.0.4
* Pageview definition counts 304s.
* refinery-core now contains a LegacyPageview class with which to classify legacy pageviews from webrequest data
* refinery-hive includes IsLegacyPageviewUDF to use legacy Pageview classification logic in hive. Also UDFs to get webrequest's access method, extract values from the X-Analytics header and determine whether or not the request came from a crawler, and geocoding UDFs got added.

## v0.0.3
* refinery-core now contains a Pageview class with which to classify pageviews from webrequest data
* refinery-hive includes IsPageviewUDF to use Pageview classification logic in hive
