package org.wikimedia.analytics.refinery.cassandra;


import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

public class TestReducerToCassandra {

    private Configuration configurationMock = Mockito.mock(Configuration.class);
    private Reducer.Context reduceContextMock = Mockito.mock(Reducer.Context.class);

    @Before
    public void setUp() {
        Mockito.reset(configurationMock);
        Mockito.reset(reduceContextMock);
    }

    @Test
    public void testReducerCore() throws IOException, InterruptedException {

        ReducerToCassandra reducer = new ReducerToCassandra();

        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_SEPARATOR_PROP)).thenReturn(" ");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_PROP)).thenReturn("i1,i2,i3");
        Mockito.when(configurationMock.get(ReducerToCassandra.INPUT_FIELDS_TYPES_PROP)).thenReturn("text,text,int");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_FIELDS_PROP)).thenReturn("i1,i4");
        Mockito.when(configurationMock.get(ReducerToCassandra.OUTPUT_PRIMARY_KEYS_PROP)).thenReturn("i2,i3");
        Mockito.when(configurationMock.get("i4")).thenReturn(",text");

        Mockito.when(reduceContextMock.getConfiguration()).thenReturn(configurationMock);

        reducer.setup(reduceContextMock);

        LongWritable key = new LongWritable();
        ArrayList<Text> values = new ArrayList<>();
        values.add(new Text("v11 v21 31"));
        values.add(new Text("v12 v22 32"));

        reducer.reduce(key, values, reduceContextMock);

        Map<String, ByteBuffer> expectedPrimaryKeysOutput1 = new LinkedHashMap<>();
        expectedPrimaryKeysOutput1.put("i2", ByteBufferUtil.bytes("v21"));
        expectedPrimaryKeysOutput1.put("i3", ByteBufferUtil.bytes(31));
        Map<String, ByteBuffer> expectedPrimaryKeysOutput2 = new LinkedHashMap<>();
        expectedPrimaryKeysOutput2.put("i2", ByteBufferUtil.bytes("v22"));
        expectedPrimaryKeysOutput2.put("i3", ByteBufferUtil.bytes(32));

        List<ByteBuffer> expectedFieldsOutput1 = new ArrayList<>();
        expectedFieldsOutput1.add(ByteBufferUtil.bytes("v11"));
        expectedFieldsOutput1.add(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        List<ByteBuffer> expectedFieldsOutput2 = new ArrayList<>();
        expectedFieldsOutput2.add(ByteBufferUtil.bytes("v12"));
        expectedFieldsOutput2.add(ByteBufferUtil.EMPTY_BYTE_BUFFER);

        Mockito.verify(reduceContextMock).getConfiguration();
        Mockito.verify(reduceContextMock).write(
                Matchers.eq(expectedPrimaryKeysOutput1), Matchers.eq(expectedFieldsOutput1));
        Mockito.verify(reduceContextMock).write(
                Matchers.eq(expectedPrimaryKeysOutput2), Matchers.eq(expectedFieldsOutput2));
        Mockito.verifyNoMoreInteractions(reduceContextMock);

    }


}
