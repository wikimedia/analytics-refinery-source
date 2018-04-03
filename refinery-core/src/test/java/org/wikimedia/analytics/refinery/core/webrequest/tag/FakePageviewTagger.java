package org.wikimedia.analytics.refinery.core.webrequest.tag;

import org.wikimedia.analytics.refinery.core.PageviewDefinition;
import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by nuriaruiz on 6/1/17.
 *
 * Testing whether this tagger can "tag" pageviews
 * No need for this code to run in prod quite yet
 * Some "fake" execution stage order
 */
@Tag(tag = {"fake-pageview"}, executionStage = 2)
public class FakePageviewTagger implements Tagger {
    @Override
    public Set<String> getTags(WebrequestData webrequestData, Set<String> tagAccumulator){

        Set <String> tags = new HashSet<>();

        if (PageviewDefinition.getInstance().isPageview(webrequestData)){
            tags.add("fake-pageview");
        }
        return tags;
    }
}
