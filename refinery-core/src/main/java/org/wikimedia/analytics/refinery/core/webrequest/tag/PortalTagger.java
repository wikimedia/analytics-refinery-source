package org.wikimedia.analytics.refinery.core.webrequest.tag;

import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Tags requests made to various wikimedia project portals,
 * such as wwww.wikipedia.org.
 * [executionStage = 0] because this tagger does not depend on other tags
 * found in tagAccumulator.
 */
@Tag(tag = {"portal"}, executionStage = 0)
public class PortalTagger implements Tagger {

    Set<String> portalDomains = new HashSet<String>(
        Arrays.asList("www.wikimedia.org",
            "www.wikipedia.org",
            "www.wiktionary.org",
            "www.wikiquote.org",
            "www.wikibooks.org",
            "www.wikinews.org",
            "www.wikiversity.org",
            "www.wikivoyage.org"));

    public Set<String> getTags(WebrequestData webrequestData, Set<String> tagAccumulator){

        Set<String>   tags = new HashSet<>();

        assert webrequestData.getUriPath()!=null: webrequestData;

        if (webrequestData.getUriPath().equals("/")
            && webrequestData.getContentType().startsWith("text/html") ) {


            if (portalDomains.contains(webrequestData.getUriHost())) {
                tags.add("portal");
            }

        }
        return tags;

    }

}