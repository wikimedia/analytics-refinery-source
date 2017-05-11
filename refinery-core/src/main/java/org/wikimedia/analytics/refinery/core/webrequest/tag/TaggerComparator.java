package org.wikimedia.analytics.refinery.core.webrequest.tag;

/**
 * Created by nuriaruiz on 6/30/17.
 */
import java.util.Comparator;

/**
 * Used to sort tags depending on executionStage
 */
public class TaggerComparator implements Comparator<Tagger> {
    /**
     * Returns a negative integer, zero, or a positive integer as
     * the first argument is less than, equal to, or greater than the second.
     * @param t1
     * @param t2
     * @return
     */
    @Override
    public int compare(Tagger t1, Tagger t2){

        return t1.getClass().getAnnotation(Tag.class).executionStage()  -
             t2.getClass().getAnnotation(Tag.class).executionStage();
    }
}

