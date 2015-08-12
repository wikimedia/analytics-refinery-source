package org.wikimedia.analytics.refinery.cassandra;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Created by jo on 8/5/15.
 */
public class TestCassandraXSVLoader {

    private Configuration configurationMock = Mockito.mock(Configuration.class);
    private Reducer.Context reduceContextMock = Mockito.mock(Reducer.Context.class);

    @Before
    public void setUp() {
        Mockito.reset(configurationMock);
        Mockito.reset(reduceContextMock);
    }

    @Test
    public void testReducerSetup() throws IOException {

        ReducerToCassandra reducer = new ReducerToCassandra();

        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_SEPARATOR_PROP)).thenReturn(" ");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_PROP)).thenReturn("i1,i2,i3");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_TYPES_PROP)).thenReturn("text,text,int");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_FIELDS_PROP)).thenReturn("i1,i4");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_PRIMARY_KEYS_PROP)).thenReturn("i2,i3");
        Mockito.when(configurationMock.get("i4")).thenReturn(",text");

        Mockito.when(reduceContextMock.getConfiguration()).thenReturn(configurationMock);

        reducer.setup(reduceContextMock);

        Mockito.verify(configurationMock).get(Matchers.eq(ReducerToCassandra.INPUT_SEPARATOR_PROP));
        Mockito.verify(configurationMock).get(Matchers.eq(ReducerToCassandra.INPUT_FIELDS_PROP));
        Mockito.verify(configurationMock).get(Matchers.eq(ReducerToCassandra.INPUT_FIELDS_TYPES_PROP));
        Mockito.verify(configurationMock).get(Matchers.eq(ReducerToCassandra.OUTPUT_FIELDS_PROP));
        Mockito.verify(configurationMock).get(Matchers.eq(ReducerToCassandra.OUTPUT_PRIMARY_KEYS_PROP));
        Mockito.verify(configurationMock).get(Matchers.eq("i4"));
        Mockito.verifyNoMoreInteractions(configurationMock);

    }

    @Test
    public void testMakeCqlQuery() throws IOException, InterruptedException {

        CassandraXSVLoader runner = new CassandraXSVLoader();

        Mockito.when(configurationMock.get(CassandraXSVLoader.OUTPUT_KEYSPACE_PROP)).thenReturn("ks");
        Mockito.when(configurationMock.get(CassandraXSVLoader.OUTPUT_COLUMN_FAMILY_PROP)).thenReturn("cf");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_FIELDS_PROP)).thenReturn("i2,i3,i4");

        runner.setConf(configurationMock);

        String computedCqlQuery = runner.makeCqlQuery();

        String expectedCqlQuery = "UPDATE \"ks\".\"cf\" SET \"i2\" = ?, \"i3\" = ?, \"i4\" = ?";

        assertEquals(computedCqlQuery, expectedCqlQuery);

    }

    @Test
    public void testCheckConfParameters_EmptyConf() throws IOException, InterruptedException {
        CassandraXSVLoader runner = new CassandraXSVLoader();
        runner.setConf(configurationMock);
        assertFalse(runner.checkConfParameters());
    }

    @Test
    public void testCheckConfParameters_WrongInputFieldsTypes() throws IOException, InterruptedException {

        CassandraXSVLoader runner = new CassandraXSVLoader();

        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_HOST_PROP)).thenReturn("host");
        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_USER_PROP)).thenReturn("user");
        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_PASSWD_PROP)).thenReturn("passwd");
        Mockito.when(configurationMock.get(CassandraXSVLoader.INPUT_PATH_PROP)).thenReturn("path");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_SEPARATOR_PROP)).thenReturn("sep");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_PROP)).thenReturn("i1,i2,i3");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_TYPES_PROP)).thenReturn("text,text");
        Mockito.when(configurationMock.get(CassandraXSVLoader.OUTPUT_KEYSPACE_PROP)).thenReturn("ks");
        Mockito.when(configurationMock.get(CassandraXSVLoader.OUTPUT_COLUMN_FAMILY_PROP)).thenReturn("cf");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_FIELDS_PROP)).thenReturn("i1,i2,i4");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_PRIMARY_KEYS_PROP)).thenReturn("i1,i2");

        runner.setConf(configurationMock);
        assertFalse(runner.checkConfParameters());

    }

    @Test
    public void testCheckConfParameters_WrongOutputField() throws IOException, InterruptedException {

        CassandraXSVLoader runner = new CassandraXSVLoader();

        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_HOST_PROP)).thenReturn("host");
        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_USER_PROP)).thenReturn("user");
        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_PASSWD_PROP)).thenReturn("passwd");
        Mockito.when(configurationMock.get(CassandraXSVLoader.INPUT_PATH_PROP)).thenReturn("path");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_SEPARATOR_PROP)).thenReturn("sep");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_PROP)).thenReturn("i1,i2,i3");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_TYPES_PROP)).thenReturn("text,text,int");
        Mockito.when(configurationMock.get(CassandraXSVLoader.OUTPUT_KEYSPACE_PROP)).thenReturn("ks");
        Mockito.when(configurationMock.get(CassandraXSVLoader.OUTPUT_COLUMN_FAMILY_PROP)).thenReturn("cf");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_FIELDS_PROP)).thenReturn("i1,i2,i4");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_PRIMARY_KEYS_PROP)).thenReturn("i1,i2");

        runner.setConf(configurationMock);
        assertFalse(runner.checkConfParameters());

    }

    @Test
    public void testCheckConfParameters_WrongPrimaryKey() throws IOException, InterruptedException {

        CassandraXSVLoader runner = new CassandraXSVLoader();

        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_HOST_PROP)).thenReturn("host");
        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_USER_PROP)).thenReturn("user");
        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_PASSWD_PROP)).thenReturn("passwd");
        Mockito.when(configurationMock.get(CassandraXSVLoader.INPUT_PATH_PROP)).thenReturn("path");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_SEPARATOR_PROP)).thenReturn("sep");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_PROP)).thenReturn("i1,i2,i3");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_TYPES_PROP)).thenReturn("text,text,int");
        Mockito.when(configurationMock.get(CassandraXSVLoader.OUTPUT_KEYSPACE_PROP)).thenReturn("ks");
        Mockito.when(configurationMock.get(CassandraXSVLoader.OUTPUT_COLUMN_FAMILY_PROP)).thenReturn("cf");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_FIELDS_PROP)).thenReturn("i1,i2,i3");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_PRIMARY_KEYS_PROP)).thenReturn("i1,i4");

        runner.setConf(configurationMock);
        assertFalse(runner.checkConfParameters());

    }

    @Test
    public void testCheckConfParameters_Ok() throws IOException, InterruptedException {

        CassandraXSVLoader runner = new CassandraXSVLoader();

        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_HOST_PROP)).thenReturn("host");
        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_USER_PROP)).thenReturn("user");
        Mockito.when(configurationMock.get(CassandraXSVLoader.CASSANDRA_PASSWD_PROP)).thenReturn("passwd");
        Mockito.when(configurationMock.get(CassandraXSVLoader.INPUT_PATH_PROP)).thenReturn("path");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_SEPARATOR_PROP)).thenReturn("sep");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_PROP)).thenReturn("i1,i2,i3");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_TYPES_PROP)).thenReturn("text,text,int");
        Mockito.when(configurationMock.get(CassandraXSVLoader.OUTPUT_KEYSPACE_PROP)).thenReturn("ks");
        Mockito.when(configurationMock.get(CassandraXSVLoader.OUTPUT_COLUMN_FAMILY_PROP)).thenReturn("cf");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_FIELDS_PROP)).thenReturn("i1,i2,i3,i4");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_PRIMARY_KEYS_PROP)).thenReturn("i1,i2");
        Mockito.when(configurationMock.get("i4")).thenReturn("value,text");

        runner.setConf(configurationMock);
        assertTrue(runner.checkConfParameters());

    }



}
