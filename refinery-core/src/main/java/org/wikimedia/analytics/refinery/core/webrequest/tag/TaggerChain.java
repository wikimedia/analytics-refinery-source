package org.wikimedia.analytics.refinery.core.webrequest.tag;

import org.reflections.Reflections;
import org.wikimedia.analytics.refinery.core.Webrequest;
import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

import java.util.*;
/**
 * Base utility class to loop through a set of taggers
 * and return an array of strings that correspond to the tags
 * that are associated to a request.
 *
 * Tags could be "portal", "pageview", "preview"
 */

public class TaggerChain {


    protected List<Tagger>  chain = new ArrayList<Tagger>();

    /**
     * Initializes the tag chain by
     * Discovering the taggers at runtime
     *
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public TaggerChain() throws ClassNotFoundException, IllegalAccessException, InstantiationException{
        // scan annotations at runtime and build chain
        Reflections reflections = new Reflections("org.wikimedia.analytics.refinery.core.webrequest.tag");
        Set<Class<?>> taggerClasses = reflections.getTypesAnnotatedWith(org.wikimedia.analytics.refinery.core.webrequest.tag.Tag.class‌​);

        for (Class taggerClass : taggerClasses) {
            Class<?> clazz;
            clazz = Class.forName(taggerClass.getName());
            chain.add((Tagger) clazz.newInstance());

            Collections.sort(chain, new TaggerComparator());
        }

    }
    /**
     * Builds the list of tags asking the "taggers"
     *
     * @return Set<String> Set of tags or empty list
     */
    public Set<String> getTags(WebrequestData webrequest){
        Set<String> tags = new HashSet<>();

        /**
         * Avoid null pointer exceptions in records like the following:
         * "hostname":null,"sequence":null,"dt":null,"time_firstbyte":null,"ip":null,
         * "cache_status":null,"http_status":"304",
         * "response_size":null,"http_method":null,"uri_host":"maps.wikimedia.org",
         * "uri_path":"/osm-intl/4/2/5.png","uri_query":"","content_type":"image/png",
         * "referer":null,"x_forwarded_for":null,
         * ...
         * "namespace_id":null,"webrequest_source":"maps","year":2017,"month":5,"day":5,"hour":1}
         */

        assert webrequest.getUriHost()!=null: webrequest;
        assert webrequest.getHttpStatus() != null: webrequest;
        assert webrequest.getUserAgent() != null: webrequest;

        // only pass to taggers healthy requests
        if(Webrequest.SUCCESS_HTTP_STATUSES.contains(webrequest.getHttpStatus()) ) {
            for (Tagger t : this.chain) {
                Set<String> newTags = t.getTags(webrequest, tags);
                tags.addAll(newTags);
            }
        }
            return tags;
    }

}
