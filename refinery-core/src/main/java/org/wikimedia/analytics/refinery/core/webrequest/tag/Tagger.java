package org.wikimedia.analytics.refinery.core.webrequest.tag;


import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

import java.util.Set;

/**
 *
 * Defines the method(s) that taggers need to implement
 *
 * Given a set of attributes from the request it returns a set of tags.
 * Set will be empty if no tag is found, also set might just contain one element
 *
 * We also pass the tags thus far so no logic needs to be repeated, for example
 * if you want to tag only requests that are pageviews you can look whether that
 * is been established via a prior tag. If you do so, remember to specify the
 * executionStage value in the @Tag annotation to guarantee execution order
 * (see Tag.java for executionStage description).
 *
 * @return  String
 */
public interface Tagger {

    public Set<String> getTags(WebrequestData webrequestData, Set<String> tagAccumulator);
}
