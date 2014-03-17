/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.metrics.HintedHandoffMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * The hint schema looks like this:
 *
 * CREATE TABLE hints (
 *   target_id uuid,
 *   hint_id timeuuid,
 *   message_version int,
 *   mutation blob,
 *   PRIMARY KEY (target_id, hint_id, message_version)
 * ) WITH COMPACT STORAGE;
 *
 * Thus, for each node in the cluster we treat its uuid as the partition key; each hint is a logical row
 * (physical composite column) containing the mutation to replay and associated metadata.
 *
 * When FailureDetector signals that a node that was down is back up, we page through
 * the hinted mutations and send them over one at a time, waiting for
 * hinted_handoff_throttle_delay in between each.
 *
 * deliverHints is also exposed to JMX so it can be run manually if FD ever misses
 * its cue somehow.
 */

public class HintedHandOffManager implements HintedHandOffManagerMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=HintedHandoffManager";
    public static final HintedHandOffManager instance = new HintedHandOffManager();

    private static final Logger logger = LoggerFactory.getLogger(HintedHandOffManager.class);
    private static final int PAGE_SIZE = 128;

    public final HintedHandoffMetrics metrics = new HintedHandoffMetrics();

    private volatile boolean hintedHandOffPaused = false;

    static final int maxHintTTL = Integer.parseInt(System.getProperty("cassandra.maxHintTTL", String.valueOf(Integer.MAX_VALUE)));

    private final NonBlockingHashSet<InetAddress> queuedDeliveries = new NonBlockingHashSet<InetAddress>();

    private final ThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getMaxHintsThread(),
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.SECONDS,
                                                                                 new LinkedBlockingQueue<Runnable>(),
                                                                                 new NamedThreadFactory("HintedHandoff", Thread.MIN_PRIORITY),
                                                                                 "internal");

    private final ColumnFamilyStore hintStore = Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(SystemKeyspace.HINTS_CF);
    
    /**
     * DELETE from hints USING TIMESTAMP ? WHERE target_id = ? AND hint_id = ? AND message_version = ?
     */
    private final DeleteStatement deleteHint;
    
    /**
     * INSERT INTO hints (target_id, hint_id, message_version, mutation) VALUES (?, now(), ?, ?) USING TTL ?
     */
    private final CQLStatement insertHint;
    
    /**
     * "SELECT target_id, hint_id, message_version, mutation, WRITETIME(mutation), TTL(mutation) FROM hints WHERE target_id = ?"
     */
    private final SelectStatement selectHints;
    
    public HintedHandOffManager()
    {
        super();
        try
        {
            deleteHint = (DeleteStatement) QueryProcessor.parseStatement(
                    String.format("DELETE from %s.%s USING TIMESTAMP ? WHERE target_id = ? AND hint_id = ? AND message_version = ?", 
                            Keyspace.SYSTEM_KS, SystemKeyspace.HINTS_CF),
                    QueryState.forInternalCalls());
            
            insertHint = QueryProcessor.parseStatement(
                    String.format("INSERT INTO %s.%s (target_id, hint_id, message_version, mutation) VALUES (?, now(), ?, ?) USING TTL ?", 
                            Keyspace.SYSTEM_KS, SystemKeyspace.HINTS_CF),
                    QueryState.forInternalCalls());
            
            selectHints = (SelectStatement) QueryProcessor.parseStatement(
                    String.format("SELECT target_id, hint_id, message_version, mutation, WRITETIME(mutation) as tstamp, TTL(mutation) as ttl FROM %s.%s WHERE target_id = ?", 
                            Keyspace.SYSTEM_KS, SystemKeyspace.HINTS_CF),
                    QueryState.forInternalCalls());
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }
    /**
     * Returns a mutation representing a Hint to be sent to <code>targetId</code>
     * as soon as it becomes available again.
     */
    public void insertHintFor(Mutation mutation, int ttl, UUID targetId)
    {
        assert ttl > 0;

        InetAddress endpoint = StorageService.instance.getTokenMetadata().getEndpointForHostId(targetId);
        // during tests we may not have a matching endpoint, but this would be unexpected in real clusters
        if (endpoint != null)
            metrics.incrCreatedHints(endpoint);
        else
            logger.warn("Unable to find matching endpoint for target {} when storing a hint", targetId);

        try
        {
            QueryProcessor.processPrepared(insertHint,
                    QueryState.forInternalCalls(),
                    new QueryOptions(ConsistencyLevel.ONE,
                        Lists.newArrayList(
                                ByteBufferUtil.bytes(targetId),
                                ByteBufferUtil.bytes(MessagingService.current_version),
                                ByteBuffer.wrap(FBUtilities.serialize(mutation, Mutation.serializer, MessagingService.current_version)),
                                ByteBufferUtil.bytes(ttl)
                        )
                   )
           );
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    /*
     * determine the TTL for the hint Mutation
     * this is set at the smallest GCGraceSeconds for any of the CFs in the RM
     * this ensures that deletes aren't "undone" by delivery of an old hint
     */
    public static int calculateHintTTL(Mutation mutation)
    {
        int ttl = maxHintTTL;
        for (ColumnFamily cf : mutation.getColumnFamilies())
            ttl = Math.min(ttl, cf.metadata().getGcGraceSeconds());
        return ttl;
    }


    public void start()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        logger.debug("Created HHOM instance, registered MBean.");

        Runnable runnable = new Runnable()
        {
            public void run()
            {
                scheduleAllDeliveries();
                metrics.log();
            }
        };
        StorageService.optionalTasks.scheduleWithFixedDelay(runnable, 10, 10, TimeUnit.MINUTES);
    }

    private void deleteHint(Row hint)
    {
        try
        {
            QueryProcessor.processPrepared(deleteHint,
                    QueryState.forInternalCalls(),
                    new QueryOptions(ConsistencyLevel.ONE,
                            Lists.newArrayList(
                                    hint.getBytes("tstamp"),
                                    hint.getBytes("target_id"),
                                    hint.getBytes("hint_id"),
                                    hint.getBytes("message_version")
                                    )
                    )
            );
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void deleteHintsForEndpoint(final String ipOrHostname)
    {
        try
        {
            InetAddress endpoint = InetAddress.getByName(ipOrHostname);
            deleteHintsForEndpoint(endpoint);
        }
        catch (UnknownHostException e)
        {
            logger.warn("Unable to find {}, not a hostname or ipaddr of a node", ipOrHostname);
            throw new RuntimeException(e);
        }
    }

    public void deleteHintsForEndpoint(final InetAddress endpoint)
    {
        if (!StorageService.instance.getTokenMetadata().isMember(endpoint))
            return;
        final UUID hostId = StorageService.instance.getTokenMetadata().getHostId(endpoint);

        // execute asynchronously to avoid blocking caller (which may be processing gossip)
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                try
                {
                    logger.info("Deleting any stored hints for {}", endpoint);
                    QueryProcessor.processInternal(String.format("DELETE FROM %s.%s WHERE target_id = %s", 
                            Keyspace.SYSTEM_KS, SystemKeyspace.HINTS_CF, hostId));
                    compact();
                }
                catch (Exception e)
                {
                    logger.warn("Could not delete hints for {}: {}", endpoint, e);
                }
            }
        };
        StorageService.optionalTasks.submit(runnable);
    }

    public void truncateAllHints() throws ExecutionException, InterruptedException
    {
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                try
                {
                    logger.info("Truncating all stored hints.");
                    Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(SystemKeyspace.HINTS_CF).truncateBlocking();
                }
                catch (Exception e)
                {
                    logger.warn("Could not truncate all hints.", e);
                }
            }
        };
        StorageService.optionalTasks.submit(runnable).get();

    }

    @VisibleForTesting
    protected Future<?> compact()
    {
        hintStore.forceBlockingFlush();
        ArrayList<Descriptor> descriptors = new ArrayList<>();
        for (SSTable sstable : hintStore.getSSTables())
            descriptors.add(sstable.descriptor);
        return CompactionManager.instance.submitUserDefined(hintStore, descriptors, (int) (System.currentTimeMillis() / 1000));
    }

    private int waitForSchemaAgreement(InetAddress endpoint) throws TimeoutException
    {
        Gossiper gossiper = Gossiper.instance;
        int waited = 0;
        // first, wait for schema to be gossiped.
        while (gossiper.getEndpointStateForEndpoint(endpoint) != null && gossiper.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.SCHEMA) == null)
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            waited += 1000;
            if (waited > 2 * StorageService.RING_DELAY)
                throw new TimeoutException("Didin't receive gossiped schema from " + endpoint + " in " + 2 * StorageService.RING_DELAY + "ms");
        }
        if (gossiper.getEndpointStateForEndpoint(endpoint) == null)
            throw new TimeoutException("Node " + endpoint + " vanished while waiting for agreement");
        waited = 0;
        // then wait for the correct schema version.
        // usually we use DD.getDefsVersion, which checks the local schema uuid as stored in the system keyspace.
        // here we check the one in gossip instead; this serves as a canary to warn us if we introduce a bug that
        // causes the two to diverge (see CASSANDRA-2946)
        while (gossiper.getEndpointStateForEndpoint(endpoint) != null && !gossiper.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.SCHEMA).value.equals(
                gossiper.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress()).getApplicationState(ApplicationState.SCHEMA).value))
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            waited += 1000;
            if (waited > 2 * StorageService.RING_DELAY)
                throw new TimeoutException("Could not reach schema agreement with " + endpoint + " in " + 2 * StorageService.RING_DELAY + "ms");
        }
        if (gossiper.getEndpointStateForEndpoint(endpoint) == null)
            throw new TimeoutException("Node " + endpoint + " vanished while waiting for agreement");
        logger.debug("schema for {} matches local schema", endpoint);
        return waited;
    }

    private void deliverHintsToEndpoint(InetAddress endpoint) throws RequestExecutionException, RequestValidationException
    {
        if (hintStore.isEmpty())
            return; // nothing to do, don't confuse users by logging a no-op handoff

        // check if hints delivery has been paused
        if (hintedHandOffPaused)
        {
            logger.debug("Hints delivery process is paused, aborting");
            return;
        }

        logger.debug("Checking remote({}) schema before delivering hints", endpoint);
        try
        {
            waitForSchemaAgreement(endpoint);
        }
        catch (TimeoutException e)
        {
            return;
        }

        if (!FailureDetector.instance.isAlive(endpoint))
        {
            logger.debug("Endpoint {} died before hint delivery, aborting", endpoint);
            return;
        }

        doDeliverHintsToEndpoint(endpoint);
    }

    /*
     * 1. Get the key of the endpoint we need to handoff
     * 2. For each column, deserialize the mutation and send it to the endpoint
     * 3. Delete the subcolumn if the write was successful
     * 4. Force a flush
     * 5. Do major compaction to clean up all deletes etc.
     */
    private void doDeliverHintsToEndpoint(InetAddress endpoint) throws RequestExecutionException, RequestValidationException
    {
        // find the hints for the node using its token.
        final UUID hostId = Gossiper.instance.getHostId(endpoint);
        logger.info("Started hinted handoff for host: {} with IP: {}", hostId, endpoint);
        final ByteBuffer hostIdBytes = ByteBufferUtil.bytes(hostId);

        final AtomicInteger rowsReplayed = new AtomicInteger(0);

        int pageSize = calculatePageSize();
        logger.debug("Using pageSize of {}", pageSize);

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (CASSANDRA-5272).
        int throttleInKB = DatabaseDescriptor.getHintedHandoffThrottleInKB()
                           / (StorageService.instance.getTokenMetadata().getAllEndpoints().size() - 1);
        RateLimiter rateLimiter = RateLimiter.create(throttleInKB == 0 ? Double.MAX_VALUE : throttleInKB * 1024);

        boolean finished = false;
        PagingState pagingState = null;
        delivery:
        while (true)
        {
            
            ResultMessage result = QueryProcessor.processPrepared(selectHints, QueryState.forInternalCalls(), 
                    new QueryOptions(ConsistencyLevel.ONE, Lists.newArrayList(hostIdBytes), false, pageSize, pagingState, null));
            if (!(result instanceof ResultMessage.Rows))
            {
                logger.info("Finished hinted handoff of {} rows to endpoint {}", rowsReplayed, endpoint);
                finished = true;
                break;
            }
            
            UntypedResultSet hintsPage = UntypedResultSet.create(((ResultMessage.Rows)result).result);
            if (hintsPage.isEmpty())
            {
                logger.info("Finished hinted handoff of {} rows to endpoint {}", rowsReplayed, endpoint);
                finished = true;
                break;
            }
            pagingState = ((ResultMessage.Rows)result).result.metadata.pagingState;

            // check if node is still alive and we should continue delivery process
            if (!FailureDetector.instance.isAlive(endpoint))
            {
                logger.info("Endpoint {} died during hint delivery; aborting ({} delivered)", endpoint, rowsReplayed);
                break;
            }

            List<WriteResponseHandler> responseHandlers = Lists.newArrayList();
            Map<UUID, Long> truncationTimesCache = new HashMap<>();
            for (final Row hint : hintsPage)
            {
                // check if hints delivery has been paused during the process
                if (hintedHandOffPaused)
                {
                    logger.debug("Hints delivery process is paused, aborting");
                    break delivery;
                }

                int version = hint.getInt("message_version");
                long writetime = hint.getLong("tstamp");
                Integer ttl = hint.has("ttl") ? hint.getInt("ttl") : null;
                
                // Skip tombstones:
                // if we iterate quickly enough, it's possible that we could request a new page in the same millisecond
                // in which the local deletion timestamp was generated on the last column in the old page, in which
                // case the hint will have no columns (since it's deleted) but will still be included in the resultset
                // since (even with gcgs=0) it's still a "relevant" tombstone.
                //FIXME: writetime - nanos, ttl - secs
                if ((ttl != null) && ((writetime + ttl*10e9)/1000 < System.currentTimeMillis()))
                    continue;

                DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(hint.getBytes("mutation")));
                Mutation mutation;
                try
                {
                    mutation = Mutation.serializer.deserialize(in, version);
                }
                catch (UnknownColumnFamilyException e)
                {
                    logger.debug("Skipping delivery of hint for deleted columnfamily", e);
                    deleteHint(hint);
                    continue;
                }
                catch (IOException e)
                {
                    throw new AssertionError(e);
                }

                truncationTimesCache.clear();
                for (UUID cfId : ImmutableSet.copyOf((mutation.getColumnFamilyIds())))
                {
                    Long truncatedAt = truncationTimesCache.get(cfId);
                    if (truncatedAt == null)
                    {
                        ColumnFamilyStore cfs = Keyspace.open(mutation.getKeyspaceName()).getColumnFamilyStore(cfId);
                        truncatedAt = cfs.getTruncationTime();
                        truncationTimesCache.put(cfId, truncatedAt);
                    }
                    //TODO: doublecheck writetime- nanos
                    if (writetime < truncatedAt)
                    {
                        logger.debug("Skipping delivery of hint for truncated columnfamily {}", cfId);
                        mutation = mutation.without(cfId);
                    }
                }

                if (mutation.isEmpty())
                {
                    deleteHint(hint);
                    continue;
                }

                MessageOut<Mutation> message = mutation.createMessage();
                rateLimiter.acquire(message.serializedSize(MessagingService.current_version));
                Runnable callback = new Runnable()
                {
                    public void run()
                    {
                        rowsReplayed.incrementAndGet();
                        deleteHint(hint);
                    }
                };
                WriteResponseHandler responseHandler = new WriteResponseHandler(endpoint, WriteType.UNLOGGED_BATCH, callback);
                MessagingService.instance().sendRR(message, endpoint, responseHandler);
                responseHandlers.add(responseHandler);
            }

            for (WriteResponseHandler handler : responseHandlers)
            {
                try
                {
                    handler.get();
                }
                catch (WriteTimeoutException e)
                {
                    logger.info("Timed out replaying hints to {}; aborting ({} delivered)", endpoint, rowsReplayed);
                    break delivery;
                }
            }
        }

        if (finished || rowsReplayed.get() >= DatabaseDescriptor.getTombstoneWarnThreshold())
        {
            try
            {
                compact().get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    private int calculatePageSize()
    {
        // read less columns (mutations) per page if they are very large
        int meanColumnCount = hintStore.getMeanColumns();
        if (meanColumnCount > 0)
        {
            int averageColumnSize = (int) (hintStore.getMeanRowSize() / meanColumnCount);
            // page size of 1 does not allow actual paging b/c of >= behavior on startColumn
            return Math.max(2, Math.min(PAGE_SIZE, DatabaseDescriptor.getInMemoryCompactionLimit() / averageColumnSize));
        }
        else
        {
            return PAGE_SIZE;
        }
    }

    /**
     * Attempt delivery to any node for which we have hints.  Necessary since we can generate hints even for
     * nodes which are never officially down/failed.
     */
    private void scheduleAllDeliveries()
    {
        logger.debug("Started scheduleAllDeliveries");

        UntypedResultSet result = QueryProcessor.processInternal(
                String.format("SELECT DISTINCT target_id FROM %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.HINTS_CF));
        for (UntypedResultSet.Row row : result)
        {
            UUID hostId = row.getUUID("target_id");
            InetAddress target = StorageService.instance.getTokenMetadata().getEndpointForHostId(hostId);
            // token may have since been removed (in which case we have just read back a tombstone)
            if (target != null)
                scheduleHintDelivery(target);
        }

        logger.debug("Finished scheduleAllDeliveries");
    }

    /*
     * This method is used to deliver hints to a particular endpoint.
     * When we learn that some endpoint is back up we deliver the data
     * to him via an event driven mechanism.
    */
    public void scheduleHintDelivery(final InetAddress to)
    {
        // We should not deliver hints to the same host in 2 different threads
        if (!queuedDeliveries.add(to))
            return;

        logger.debug("Scheduling delivery of Hints to {}", to);

        executor.execute(new Runnable()
        {
            public void run()
            {
                try
                {
                    deliverHintsToEndpoint(to);
                }
                catch (RequestValidationException e)
                {
                    throw new AssertionError(e); // not supposed to happen
                }
                catch (RequestExecutionException e)
                {
                    throw new RuntimeException(e);
                }
                finally
                {
                    queuedDeliveries.remove(to);
                }
            }
        });
    }

    public void scheduleHintDelivery(String to) throws UnknownHostException
    {
        scheduleHintDelivery(InetAddress.getByName(to));
    }

    public void pauseHintsDelivery(boolean b)
    {
        hintedHandOffPaused = b;
    }

    public List<String> listEndpointsPendingHints()
    {
        // Extract the keys as strings to be reported.
        LinkedList<String> result = new LinkedList<String>();
        for (UntypedResultSet.Row row : QueryProcessor.processInternal(
                String.format("SELECT DISTINCT TOKEN(target_id) AS ttoken from %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.HINTS_CF)))
        {
            result.addFirst(row.getString("ttoken"));
        }
        return result;
    }
}
