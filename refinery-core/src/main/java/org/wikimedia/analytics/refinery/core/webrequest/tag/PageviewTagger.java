package org.wikimedia.analytics.refinery.core.webrequest.tag;

import org.wikimedia.analytics.refinery.core.PageviewDefinition;
import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

import java.util.HashSet;
import java.util.Set;

/**
 * Tags requests evaluated as pageviews by [org.wikimedia.analytics.refinery.core.PageviewDefinition]
 * [executionStage = 0] because this tagger does not depend on other tags
 * found in tagAccumulator.
 */
@Tag(tag = {"pageview"}, executionStage = 0)
public class PageviewTagger implements Tagger {

    public Set<String> getTags(WebrequestData webrequestData, Set<String> tagAccumulator){

        Set<String> tags = new HashSet<>();

        if (PageviewDefinition.getInstance().isPageview(webrequestData)) {
            tags.add("pageview");
        }

        return tags;

    }

}