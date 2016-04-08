package org.wikimedia.analytics.refinery.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * WARNING
 * This class is a copy of org.apache.cassandra.hadoop.cql3.CqlRecordReader
 * It is needed to bypass a bug (https://issues.apache.org/jira/browse/CASSANDRA-10481)
 * on lines 104-107 of this file. It was not possible to override the original class since
 * it is private in its package.
 * TODO: In case the bug gets corrected, please use the new version of the original class and remove this one.
 */
public class CqlRecordWriter extends RecordWriter<Map<String, ByteBuffer>, List<ByteBuffer>> implements
        org.apache.hadoop.mapred.RecordWriter<Map<String, ByteBuffer>, List<ByteBuffer>>, AutoCloseable
    {
        private static final Logger logger = LoggerFactory.getLogger(CqlRecordWriter.class);

        // The configuration this writer is associated with.
        protected final Configuration conf;
        // The number of mutations to buffer per endpoint
        protected final int queueSize;

        protected final long batchThreshold;

        protected Progressable progressable;
        protected TaskAttemptContext context;

        // The ring cache that describes the token ranges each node in the ring is
        // responsible for. This is what allows us to group the mutations by
        // the endpoints they should be targeted at. The targeted endpoint
        // essentially
        // acts as the primary replica for the rows being affected by the mutations.
        private final NativeRingCache ringCache;

        // handles for clients for each range running in the threadpool
        protected final Map<InetAddress, RangeClient> clients;

        // host to prepared statement id mappings
        protected final ConcurrentHashMap<Session, PreparedStatement> preparedStatements = new ConcurrentHashMap<Session, PreparedStatement>();

        protected final String cql;

        protected List<ColumnMetadata> partitionKeyColumns;
        protected List<ColumnMetadata> clusterColumns;

        /**
         * Upon construction, obtain the map that this writer will use to collect
         * mutations, and the ring cache for the given keyspace.
         *
         * @param context the task attempt context
         * @throws IOException
         */
        CqlRecordWriter(TaskAttemptContext context) throws IOException
        {
            this(HadoopCompat.getConfiguration(context));
            this.context = context;
        }

        CqlRecordWriter(Configuration conf, Progressable progressable)
        {
            this(conf);
            this.progressable = progressable;
        }

        CqlRecordWriter(Configuration conf)
        {
            logger.debug("Constructing new MultiThreadCqlRecordWriter");
            this.conf = conf;
            this.queueSize = conf.getInt(ColumnFamilyOutputFormat.QUEUE_SIZE, 32 * FBUtilities.getAvailableProcessors());
            batchThreshold = conf.getLong(ColumnFamilyOutputFormat.BATCH_THRESHOLD, 32);
            this.clients = new HashMap<>();

            try
            {
                String keyspace = ConfigHelper.getOutputKeyspace(conf);
                try (Session client = CqlConfigHelper.getOutputCluster(ConfigHelper.getOutputInitialAddress(conf), conf).connect(keyspace))
                {
                    ringCache = new NativeRingCache(conf);
                    if (client != null)
                    {
                        /** BUG
                         * In the following code line, the keyspace value was client.getLoggedKeyspace()
                         * It generated a bug because of multiple uses of Metadata.handleId(keyspace)
                         * over the same value, removing quotes and making the keyspace lower case.
                         */
                        TableMetadata tableMetadata = client.getCluster().getMetadata().getKeyspace(keyspace).getTable(ConfigHelper.getOutputColumnFamily(conf));
                        clusterColumns = tableMetadata.getClusteringColumns();
                        partitionKeyColumns = tableMetadata.getPartitionKey();

                        String cqlQuery = CqlConfigHelper.getOutputCql(conf).trim();
                        if (cqlQuery.toLowerCase().startsWith("insert"))
                            throw new UnsupportedOperationException("INSERT with CqlRecordWriter is not supported, please use UPDATE/DELETE statement");
                        cql = appendKeyWhereClauses(cqlQuery);
                    }
                    else
                    {
                        throw new IllegalArgumentException("Invalid configuration specified " + conf);
                    }
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        /**
         * Close this <code>RecordWriter</code> to future operations, but not before
         * flushing out the batched mutations.
         *
         * @param context the context of the task
         * @throws IOException
         */
        public void close(TaskAttemptContext context) throws IOException, InterruptedException
        {
            close();
        }

        /** Fills the deprecated RecordWriter interface for streaming. */
        @Deprecated
        public void close(org.apache.hadoop.mapred.Reporter reporter) throws IOException
        {
            close();
        }

        @Override
        public void close() throws IOException
        {
            // close all the clients before throwing anything
            logger.debug("Closing MultiThreadCqlRecordWriter");
            IOException clientException = null;
            for (RangeClient client : clients.values())
            {
                try
                {
                    client.close();
                }
                catch (IOException e)
                {
                    clientException = e;
                }
            }

            if (clientException != null)
                throw clientException;
        }


        @Override
        public void write(Map<String, ByteBuffer> keyColumns, List<ByteBuffer> values) throws IOException
        {
            logger.debug("Writing with MultiThreadCqlRecordWriter");
            TokenRange range = ringCache.getRange(getPartitionKey(keyColumns));

            // get the client for the given range, or create a new one
            final InetAddress address = ringCache.getEndpoints(range).get(0);
            RangeClient client = clients.get(address);
            if (client == null)
            {
                // haven't seen keys for this range: create new client
                client = new RangeClient(ringCache.getEndpoints(range));
                client.start();
                clients.put(address, client);
            }

            // add primary key columns to the bind variables
            List<ByteBuffer> allValues = new ArrayList<ByteBuffer>(values);
            for (ColumnMetadata column : partitionKeyColumns)
                allValues.add(keyColumns.get(column.getName()));
            for (ColumnMetadata column : clusterColumns)
                allValues.add(keyColumns.get(column.getName()));

            client.put(allValues);

            if (progressable != null)
                progressable.progress();
            if (context != null)
                HadoopCompat.progress(context);
        }

        /**
         * A client that runs in a threadpool and connects to the list of endpoints for a particular
         * range. Bound variables for keys in that range are sent to this client via a queue.
         */
        public class RangeClient extends Thread
        {
            protected final String name;
            // The list of endpoints for this range
            protected final List<InetAddress> endpoints;
            protected Session client;
            // A bounded queue of incoming mutations for this range
            protected final BlockingQueue<List<ByteBuffer>> queue = new ArrayBlockingQueue<List<ByteBuffer>>(queueSize);

            protected volatile boolean run = true;
            // we want the caller to know if something went wrong, so we record any unrecoverable exception while writing
            // so we can throw it on the caller's stack when he calls put() again, or if there are no more put calls,
            // when the client is closed.
            protected volatile IOException lastException;

            /**
             * Constructs an {@link RangeClient} for the given endpoints.
             * @param endpoints the possible endpoints to execute the mutations on
             */
            public RangeClient(List<InetAddress> endpoints)
            {
                super("client-" + endpoints);
                this.name = "RangeClient[client-" + endpoints + "]";
                logger.debug("Constructing new RangeClient[" + name + "]");
                this.endpoints = endpoints;
            }

            /**
             * enqueues the given value to Cassandra
             */
            public void put(List<ByteBuffer> value) throws IOException
            {
                logger.debug("Putting new value in async queue");
                while (true)
                {
                    if (lastException != null)
                        throw lastException;
                    try
                    {
                        if (queue.offer(value, 100, TimeUnit.MILLISECONDS))
                            break;
                    }
                    catch (InterruptedException e)
                    {
                        throw new AssertionError(e);
                    }
                }
            }

            /**
             * Loops collecting cql binded variable values from the queue and sending to Cassandra
             */
            public void run()
            {
                outer:
                while (run || !queue.isEmpty())
                {
                    logger.debug("Async Run - Looping while (run || !queue.isEmpty) (execution loop)");
                    List<ByteBuffer> bindVariables;
                    try
                    {
                        logger.debug("Async Run - Getting first batch value to insert into cassandra");
                        bindVariables = queue.poll(1, TimeUnit.SECONDS);
                        // re-check loop condition if no data available
                        if (bindVariables == null) continue ;
                    }
                    catch (InterruptedException e)
                    {
                        // re-check loop condition after interrupt
                        continue;
                    }

                    ListIterator<InetAddress> iter = endpoints.listIterator();
                    // Initialise client if not already done
                    if ((client == null) && (!attempt_connect(iter))) break outer;

                    while (true)
                    {
                        logger.debug("Async Run - Looping inserting batches into cassandra available client");
                        // send the mutation to the last-used endpoint.
                        try
                        {
                            int i = 0;
                            PreparedStatement statement = preparedStatement(client);
                            while (bindVariables != null)
                            {
                                logger.debug("Async Run - Looping inserting value " + Integer.toString(i) + " into cassandra with selected client");
                                BoundStatement boundStatement = new BoundStatement(statement);
                                for (int columnPosition = 0; columnPosition < bindVariables.size(); columnPosition++)
                                {
                                    if (bindVariables.get(columnPosition) != null)
                                        boundStatement.setBytesUnsafe(columnPosition, bindVariables.get(columnPosition));
                                    else
                                        boundStatement.setToNull(columnPosition);
                                }
                                client.execute(boundStatement);
                                i++;

                                if (i >= batchThreshold)
                                {
                                    logger.debug("Async Run - Batch full - Breaking values insertion loop");
                                    break;
                                }
                                bindVariables = queue.poll();
                            }
                            logger.debug("Async Run - Batch full or no more values to insert - Breaking batches insertion loop");
                            break;
                        }
                        catch (Exception e)
                        {
                            logger.debug("Async Run - Error while inserting batch with selected client", e);
                            closeInternal();
                            if (!iter.hasNext())
                            {
                                logger.debug("Async Run - No other client to try to reach, breaking execution loop");
                                lastException = new IOException(e);
                                break outer;
                            }
                        }

                        // attempt to connect to a different endpoint
                        if (!attempt_connect(iter)) break outer;
                    }
                }
                // close all our connections once we are done.
                closeInternal();
            }

            private boolean attempt_connect(ListIterator<InetAddress> iter) {
                try
                {
                    logger.debug("Async Run - Try a new client from the list");
                    InetAddress address = iter.next();
                    String host = address.getHostName();
                    client = CqlConfigHelper.getOutputCluster(host, conf).connect();
                }
                catch (Exception e)
                {
                    //If connection died due to Interrupt, just try connecting to the endpoint again.
                    //There are too many ways for the Thread.interrupted() state to be cleared, so
                    //we can't rely on that here. Until the java driver gives us a better way of knowing
                    //that this exception came from an InterruptedException, this is the best solution.
                    if (canRetryDriverConnection(e))
                    {
                        logger.debug("Async Run - Error is not critical, trying with same endpoint");
                        iter.previous();
                    }
                    closeInternal();

                    // Most exceptions mean something unexpected went wrong to that endpoint, so
                    // we should try again to another.  Other exceptions (auth or invalid request) are fatal.
                    if ((e instanceof AuthenticationException || e instanceof InvalidQueryException) || !iter.hasNext())
                    {
                        logger.debug("Async Run - Error is critical, breaking execution loop", e);
                        lastException = new IOException(e);
                        return false;
                    }
                }
                return true;
            }


            /** get prepared statement id from cache, otherwise prepare it from Cassandra server*/
            private PreparedStatement preparedStatement(Session client)
            {
                logger.debug("Getting prepared statement");
                PreparedStatement statement = preparedStatements.get(client);
                if (statement == null)
                {
                    PreparedStatement result;
                    try
                    {
                        result = client.prepare(cql);
                    }
                    catch (NoHostAvailableException e)
                    {
                        throw new RuntimeException("failed to prepare cql query " + cql, e);
                    }

                    PreparedStatement previousId = preparedStatements.putIfAbsent(client, result);
                    statement = previousId == null ? result : previousId;
                }
                return statement;
            }

            public void close() throws IOException
            {
                logger.debug("Closing external");
                // stop the run loop (Sending Interrupt signal is causing driver failure, so we rely on
                // regular condition checking instead).
                // this will result in closeInternal being called by the time join() finishes.
                run = false;
                try
                {
                    this.join();
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }

                if (lastException != null)
                    throw lastException;
            }

            protected void closeInternal()
            {
                logger.debug("Closing internal");
                if (client != null)
                {
                    client.close();
                }
            }

            private boolean canRetryDriverConnection(Exception e)
            {
                logger.debug("Checking retry connection");
                if (e instanceof DriverException && e.getMessage().contains("Connection thread interrupted"))
                    return true;
                if (e instanceof NoHostAvailableException)
                {
                    if (((NoHostAvailableException) e).getErrors().values().size() == 1)
                    {
                        Throwable cause = ((NoHostAvailableException) e).getErrors().values().iterator().next();
                        if (cause != null && cause.getCause() instanceof java.nio.channels.ClosedByInterruptException)
                        {
                            return true;
                        }
                    }
                }
                return false;
            }
        }

        private ByteBuffer getPartitionKey(Map<String, ByteBuffer> keyColumns)
        {
            ByteBuffer partitionKey;
            if (partitionKeyColumns.size() > 1)
            {
                ByteBuffer[] keys = new ByteBuffer[partitionKeyColumns.size()];
                for (int i = 0; i< keys.length; i++)
                    keys[i] = keyColumns.get(partitionKeyColumns.get(i).getName());

                partitionKey = CompositeType.build(keys);
            }
            else
            {
                partitionKey = keyColumns.get(partitionKeyColumns.get(0).getName());
            }
            return partitionKey;
        }

        /**
         * add where clauses for partition keys and cluster columns
         */
        private String appendKeyWhereClauses(String cqlQuery)
        {
            String keyWhereClause = "";

            for (ColumnMetadata partitionKey : partitionKeyColumns)
                keyWhereClause += String.format("%s = ?", keyWhereClause.isEmpty() ? quote(partitionKey.getName()) : (" AND " + quote(partitionKey.getName())));
            for (ColumnMetadata clusterColumn : clusterColumns)
                keyWhereClause += " AND " + quote(clusterColumn.getName()) + " = ?";

            return cqlQuery + " WHERE " + keyWhereClause;
        }

        /** Quoting for working with uppercase */
        private String quote(String identifier)
        {
            return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
        }

        class NativeRingCache
        {
            private Map<TokenRange, Set<Host>> rangeMap;
            private Metadata metadata;
            private final IPartitioner partitioner;
            private final Configuration conf;

            public NativeRingCache(Configuration conf)
            {
                this.conf = conf;
                this.partitioner = ConfigHelper.getOutputPartitioner(conf);
                refreshEndpointMap();
            }


            private void refreshEndpointMap()
            {
                String keyspace = ConfigHelper.getOutputKeyspace(conf);
                try (Session session = CqlConfigHelper.getOutputCluster(ConfigHelper.getOutputInitialAddress(conf), conf).connect(keyspace))
                {
                    rangeMap = new HashMap<>();
                    metadata = session.getCluster().getMetadata();
                    Set<TokenRange> ranges = metadata.getTokenRanges();
                    for (TokenRange range : ranges)
                    {
                        rangeMap.put(range, metadata.getReplicas(keyspace, range));
                    }
                }
            }

            public TokenRange getRange(ByteBuffer key)
            {
                org.apache.cassandra.dht.Token t = partitioner.getToken(key);
                Token driverToken = metadata.newToken(partitioner.getTokenFactory().toString(t));
                for (TokenRange range : rangeMap.keySet())
                {
                    if (range.contains(driverToken))
                    {
                        return range;
                    }
                }

                throw new RuntimeException("Invalid token information returned by describe_ring: " + rangeMap);
            }

            public List<InetAddress> getEndpoints(TokenRange range)
            {
                Set<Host> hostSet = rangeMap.get(range);
                List<InetAddress> addresses = new ArrayList<>(hostSet.size());
                for (Host host: hostSet)
                {
                    addresses.add(host.getAddress());
                }
                return addresses;
            }
        }
    }
