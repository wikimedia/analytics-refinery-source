package org.wikimedia.analytics.refinery.core.webrequest.tag;

/**
 * Created by nuriaruiz on 5/10/17.
 * Annotation that identifies classes that belong to the TaggerChain
 * Chain is build at runtime
 */

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Tag {
    // The tag string that the tagger will apply to webrequests.
    String tag() default "";
    // If the Tagger does not depend on other tags, executionStage should be 0.
    // If the Tagger depends on other tags, executionStage should be 1 plus the
    // maximum executionStage of all its tag dependencies.
    // This ensures that tag dependencies are executed in correct order.
    int executionStage() default 0;
}
