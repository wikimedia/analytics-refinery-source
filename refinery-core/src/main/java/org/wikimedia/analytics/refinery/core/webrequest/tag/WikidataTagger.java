package org.wikimedia.analytics.refinery.core.webrequest.tag;

import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

import java.util.Set;

/**
 * Tags for Wikidata Requests
 * include wikidata.org, www.wikidata.org, m.wikidata.org
 * also include requests to wdqs
 */
@Tag(tag = {"wikidata"}, executionStage = 0)
public class WikidataTagger implements Tagger {

    static String WIKIDATA_HOST = "wikidata";
    @Override
    public Set<String> getTags(WebrequestData webrequestData, Set<String> tags){

        if (!webrequestData.getUriHost().contains(WIKIDATA_HOST)) {
            return tags;
        }
        tags.add("wikidata") ;
        return tags;


    }
}
