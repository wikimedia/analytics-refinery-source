package org.wikimedia.analytics.refinery.cassandra;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * WARNING
 * This class is a copy of org.apache.cassandra.hadoop.cql3.CqlOutputFormat
 * It is needed to bypass a bug (https://issues.apache.org/jira/browse/CASSANDRA-10481)
 * found in the org.apache.cassandra.hadoop.cql3.CqlRecordReader private class, used by this class.
 * TODO: In case the bug gets corrected, please use the new version of the original class and remove this one.
 */
public class CqlOutputFormat extends OutputFormat<Map<String, ByteBuffer>, List<ByteBuffer>>
        implements org.apache.hadoop.mapred.OutputFormat<Map<String, ByteBuffer>, List<ByteBuffer>>
{
    /**
     * Check for validity of the output-specification for the job.
     *
     * @param context
     *            information about the job
     */
    public void checkOutputSpecs(JobContext context)
    {
        checkOutputSpecs(HadoopCompat.getConfiguration(context));
    }

    protected void checkOutputSpecs(Configuration conf)
    {
        if (ConfigHelper.getOutputKeyspace(conf) == null)
            throw new UnsupportedOperationException("You must set the keyspace with setOutputKeyspace()");
        if (ConfigHelper.getOutputPartitioner(conf) == null)
            throw new UnsupportedOperationException("You must set the output partitioner to the one used by your Cassandra cluster");
        if (ConfigHelper.getOutputInitialAddress(conf) == null)
            throw new UnsupportedOperationException("You must set the initial output address to a Cassandra node");
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public void checkOutputSpecs(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job) throws IOException
    {
        checkOutputSpecs(job);
    }

    /**
     * The OutputCommitter for this format does not write any data to the DFS.
     *
     * @param context
     *            the task context
     * @return an output committer
     * @throws IOException
     * @throws InterruptedException
     */
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new NullOutputCommitter();
    }

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Deprecated
    public CqlRecordWriter getRecordWriter(org.apache.hadoop.fs.FileSystem filesystem, org.apache.hadoop.mapred.JobConf job, String name, org.apache.hadoop.util.Progressable progress) throws IOException
    {
        return new CqlRecordWriter(job, progress);
    }

    /**
     * Get the {@link RecordWriter} for the given task.
     *
     * @param context
     *            the information about the current task.
     * @return a {@link RecordWriter} to write the output for the job.
     * @throws IOException
     */
    public CqlRecordWriter getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException
    {
        return new CqlRecordWriter(context);
    }

    /**
     * An {@link OutputCommitter} that does nothing.
     */
    private static class NullOutputCommitter extends OutputCommitter
    {
        public void abortTask(TaskAttemptContext taskContext) { }

        public void cleanupJob(JobContext jobContext) { }

        public void commitTask(TaskAttemptContext taskContext) { }

        public boolean needsTaskCommit(TaskAttemptContext taskContext)
        {
            return false;
        }

        public void setupJob(JobContext jobContext) { }

        public void setupTask(TaskAttemptContext taskContext) { }
    }
}
