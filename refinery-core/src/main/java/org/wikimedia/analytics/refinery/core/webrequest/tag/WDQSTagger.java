package org.wikimedia.analytics.refinery.core.webrequest.tag;

import java.util.HashSet;
import java.util.Set;

import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

/**
 * Tags for Wikidata Query Service.
 */
@Tag(tag = {"wikidata-query", "sparql", "ldf"}, executionStage = 0)
public class WDQSTagger implements Tagger {

    /**
     * WDQS hostname.
     */
    public final static String WDQS_HOST = "query.wikidata.org";

    @Override
    public Set<String> getTags(WebrequestData webrequestData,
            Set<String> tagAccumulator) {
        Set<String> tags = new HashSet<>();

        if (!webrequestData.getUriHost().equals(WDQS_HOST)) {
            return tags;
        }

        tags.add("wikidata-query");

        if (webrequestData.getUriPath().startsWith("/sparql") || webrequestData
                .getUriPath().startsWith("/bigdata/namespace/wdq/sparql")) {
            tags.add("sparql");
        }

        if (webrequestData.getUriPath().startsWith("/bigdata/ldf")) {
            tags.add("ldf");
        }

        return tags;
    }

}
