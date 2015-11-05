package org.wikimedia.analytics.refinery.camus.schemaregistry;

import com.linkedin.camus.schemaregistry.SchemaDetails;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by nuriaruiz on 11/20/15.
 */
public class TestLocalRepoSchemaRegistry {

    LocalRepoSchemaRegistry registry = null;
    String schemaFullName = "org.wikimedia.analytics.schemas.TestSchema";

    @Before
    public void setUp() {
        registry = new LocalRepoSchemaRegistry();
        Properties props = new Properties();
        props.setProperty("org.wikimedia.analytics.schemas.TestSchema.latestRev", "0");
        registry.init(props);
    }

    @Test
    public void testGetSchemaHappyCase() {
        Schema s = registry.getSchemaByID("some_TestSchema", "0");
        Assert.assertEquals(s.getName(), "TestSchema");

    }

    @Test
    public void testGetSchemaLatestHappyCase() {
        SchemaDetails s = registry.getLatestSchemaByTopic("some_TestSchema");
        Schema schema =  (Schema) s.getSchema();
        Assert.assertEquals(schema.getName(), "TestSchema");


    }

    @Test(expected = java.lang.RuntimeException.class)
    public void testGetSchemaDoesnotExist() {
        SchemaDetails s = registry.getLatestSchemaByTopic("some_TestSBadSchema");
    }

    @Test(expected = java.lang.RuntimeException.class)
    public void testGetSchemaWhenSchemaAndRevisionDonotExist() {
        LocalRepoSchemaRegistry registry = new LocalRepoSchemaRegistry();
        Properties props = new Properties();
        props.setProperty("org.wikimedia.analytics.schemas.some_TestSBadSchema.latestRev", "0");
        registry.init(props);
        SchemaDetails s = registry.getLatestSchemaByTopic("some_TestSBadSchema");
    }

}
