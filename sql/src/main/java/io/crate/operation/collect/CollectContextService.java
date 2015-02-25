/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.collect;

import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.crate.action.sql.query.CrateSearchContext;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.service.IndexShard;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class CollectContextService implements Releasable {

    // TODO: maybe make configurable
    public static TimeValue EXPIRATION_DEFAULT = new TimeValue(5, TimeUnit.MINUTES);

    private final ReentrantLock lock;
    private final LoadingCache<UUID, LongObjectMap<CrateSearchContext>> contexts;

    public CollectContextService() {
        this.contexts = CacheBuilder.newBuilder().expireAfterAccess(
                EXPIRATION_DEFAULT.millis(),
                TimeUnit.MILLISECONDS
        ).removalListener(new RemovalListener<UUID, LongObjectMap<CrateSearchContext>>() {
            @Override
            public void onRemoval(@Nonnull RemovalNotification<UUID, LongObjectMap<CrateSearchContext>> notification) {
                onCacheRemoval(notification);
            }
        })
        .build(new CacheLoader<UUID, LongObjectMap<CrateSearchContext>>() {
            @Override
            public LongObjectMap<CrateSearchContext> load(@Nonnull UUID key) throws Exception {
                return new LongObjectOpenHashMap<>();
            }
        });
        this.lock = new ReentrantLock();
    }

    /**
     * Do the actual removal by closing the {@link Releasable}.
     * Tests require this to be done in this dedicated method, called by {@link RemovalListener}
     */
    private void onCacheRemoval(@Nonnull RemovalNotification<UUID, LongObjectMap<CrateSearchContext>> notification) {
        LongObjectMap<CrateSearchContext> removedMap = notification.getValue();
        if (removedMap != null) {
            for (ObjectCursor<CrateSearchContext> cursor : removedMap.values()){
                Releasables.close(cursor.value);
            }
        }
    }

    /**
     * Return a {@link CrateSearchContext} for given <code>jobId</code> and
     * <code>jobSearchContextShardId</code>, null if not found.
     */
    @Nullable
    public CrateSearchContext getContext(UUID jobId, long jobSearchContextShardId) {
        final LongObjectMap<CrateSearchContext> searchContexts;
        try {
            searchContexts = contexts.get(jobId);
        } catch (ExecutionException|UncheckedExecutionException e) {
            throw Throwables.propagate(e);
        }
        return searchContexts.get(jobSearchContextShardId);
    }

    /**
     * Try to find a {@link CrateSearchContext} for the same <code>jobId</code> and same shard
     * using the given <code>jobSearchContextId</code>
     * (this is a long containing of 2 integers, first one is the shardId).
     * If one is found create {@link Engine.Searcher} using the {@link Engine.Searcher} of the
     * found {@link CrateSearchContext}.
     * Otherwise create a new one.
     */
    public Engine.Searcher getOrCreateEngineSearcher(IndexShard indexShard,
                                                     UUID jobId) {
        final LongObjectMap<CrateSearchContext> searchContexts;
        try {
            searchContexts = contexts.get(jobId);
        } catch (ExecutionException|UncheckedExecutionException e) {
            throw Throwables.propagate(e);
        }
        if (searchContexts != null) {
            // lets search for contexts of the same shard id, use searcher of first one found
            long jobSearchContextShardIdLowerLimit = packJobSearchContextShardId(indexShard.shardId().id(), 0);
            long jobSearchContextShardIdUpperLimit = packJobSearchContextShardId(indexShard.shardId().id()+1, 0);
            for (LongCursor jobSearchContextShardId : searchContexts.keys()) {
                if (jobSearchContextShardId.value >= jobSearchContextShardIdLowerLimit
                        && jobSearchContextShardId.value < jobSearchContextShardIdUpperLimit) {
                    Engine.Searcher engineSearcher = searchContexts.get(jobSearchContextShardId.value).engineSearcher();
                    // increase ref of IndexReader, only use this engine search if reader is not closed
                    if (incReaderRef(engineSearcher.reader())) {
                        return engineSearcher;
                    }
                }
            }
        }
        return EngineSearcher.getSearcherWithRetry(indexShard, null);
    }

    /**
     * Return an existing {@link CrateSearchContext} for given <code>jobId</code>,
     * <code>sharId</code> and <code>jobSearchContextId</code>, if not found use given
     * <code>createSearchContextFunction</code> callable to create a new one.
     */
    public CrateSearchContext getOrCreateContext(UUID jobId,
                                                 int shardId,
                                                 int jobSearchContextId,
                                                 Callable<CrateSearchContext> createSearchContextFunction) {
        long jobSearchContextShardId = packJobSearchContextShardId(shardId, jobSearchContextId);
        try {
            final LongObjectMap<CrateSearchContext> searchContexts = contexts.get(jobId);
            lock.lock();
            try {
                CrateSearchContext searchContext = searchContexts.get(jobSearchContextShardId);
                if (searchContext == null) {
                    searchContext = createSearchContextFunction.call();
                    searchContexts.put(jobSearchContextShardId, searchContext);
                }
                return searchContext;
            } finally {
                lock.unlock();
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void close() throws ElasticsearchException {
        this.contexts.invalidateAll();
    }

    /**
     * Pack <code>shardId</code> and <code>jobSearchContextId</code> integers into one long.
     */
    private long packJobSearchContextShardId(int shardId, int jobSearchContextId) {
        return (((long) shardId) << 32) | (jobSearchContextId & 0xffffffffL);
    }

    /**
     * Wrapper method, needed by tests
     */
    private boolean incReaderRef(IndexReader reader) {
        return reader.tryIncRef();
    }

}
