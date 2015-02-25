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

import io.crate.action.sql.query.CrateSearchContext;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.search.internal.SearchContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CollectContextService.class})
public class CollectContextServiceTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    static Engine.Searcher engineSearcher = mock(Engine.Searcher.class);


    private static final Callable<CrateSearchContext> CONTEXT_FUNCTION =  new Callable<CrateSearchContext>() {

        @Nullable
        @Override
        public CrateSearchContext call() {
            CrateSearchContext crateSearchContext = mock(CrateSearchContext.class);
            when(crateSearchContext.engineSearcher()).thenReturn(engineSearcher);
            return crateSearchContext;
        }
    };

    @Test
    public void testSameForSameArgs() throws Exception {
        CollectContextService collectContextService = new CollectContextService();
        UUID jobId = UUID.randomUUID();
        SearchContext ctx1 = collectContextService.getOrCreateContext(jobId, 0, 0, CONTEXT_FUNCTION);
        SearchContext ctx2 = collectContextService.getOrCreateContext(jobId, 0, 0, CONTEXT_FUNCTION);
        assertThat(ctx1, is(ctx2));
        SearchContext ctx3 = collectContextService.getOrCreateContext(UUID.randomUUID(), 0, 0, CONTEXT_FUNCTION);
        assertThat(ctx3, is(not(ctx1)));

        SearchContext ctx4 = collectContextService.getOrCreateContext(jobId, 1, 0, CONTEXT_FUNCTION);
        assertThat(ctx4, is(not(ctx1)));
    }

    @Test
    public void testGetContext() throws Exception {
        CollectContextService collectContextService = new CollectContextService();
        UUID jobId = UUID.randomUUID();
        SearchContext ctx1 = collectContextService.getOrCreateContext(jobId, 0, 0, CONTEXT_FUNCTION);

        SearchContext ctx2 = collectContextService.getContext(jobId, 0L);
        assertThat(ctx2, is(ctx1));
    }

    @Test
    public void testPackJobSearchContextShardId() throws Exception {
        CollectContextService collectContextService = new CollectContextService();

        Method packJobSearchContextShardId = CollectContextService.class.
                getDeclaredMethod("packJobSearchContextShardId", Integer.TYPE, Integer.TYPE);
        packJobSearchContextShardId.setAccessible(true);

        long jobSearchContextShardId = (long)packJobSearchContextShardId.invoke(collectContextService, 1, 1);
        assertThat(jobSearchContextShardId, is(4294967297L));
    }

    @Test
    public void testGetOrCreateEngineSearcher() throws Exception {
        CollectContextService collectContextService = spy(new CollectContextService());
        UUID jobId = UUID.randomUUID();
        int shardId = 0;
        int jobSearchContextId = 1;

        // mocking all the things
        class RefCount {
            int refCount = 1;

            public void inc() {
                refCount++;
            }

            public int refCount() {
                return refCount;
            }
        }
        final RefCount refCount = new RefCount();
        Answer<Boolean> incRefAnswer = new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                refCount.inc();
                return true;
            }
        };
        doAnswer(incRefAnswer).when(collectContextService, "incReaderRef", any(IndexReader.class));
        Engine.Searcher engineSearcher2 = mock(Engine.Searcher.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("dummy", 0));
        when(indexShard.acquireSearcher("search")).thenReturn(engineSearcher2);

        // first no context is available, so a new Engine.Searcher will be created.
        Engine.Searcher engineSearcher1 = collectContextService.getOrCreateEngineSearcher(indexShard, jobId);
        assertEquals(engineSearcher1, engineSearcher2);

        // create context
        CrateSearchContext ctx1 = collectContextService.getOrCreateContext(jobId, shardId, jobSearchContextId, CONTEXT_FUNCTION);

        // now a context for this shard exists, we should get the same Engine.Searcher
        engineSearcher1 = collectContextService.getOrCreateEngineSearcher(indexShard, jobId);
        assertEquals(ctx1.engineSearcher(), engineSearcher1);

        // also IndexReader ref count must increase
        assertThat(refCount.refCount(), is(2));
    }

    @Test
    public void testExpire() throws Exception {
        TimeValue originalExpirationDefault = CollectContextService.EXPIRATION_DEFAULT;
        CollectContextService.EXPIRATION_DEFAULT = new TimeValue(0);
        CollectContextService collectContextService = new CollectContextService();
        UUID jobId = UUID.randomUUID();
        int shardId = 0;
        int jobSearchContextId = 1;

        // create context
        CrateSearchContext ctx1 = collectContextService.getOrCreateContext(jobId, shardId, jobSearchContextId, CONTEXT_FUNCTION);

        // another call creates new context, because old one is expired
        CrateSearchContext ctx2 = collectContextService.getOrCreateContext(jobId, shardId, jobSearchContextId, CONTEXT_FUNCTION);
        assertNotEquals(ctx1, ctx2);

        // set expiration default back to original one
        CollectContextService.EXPIRATION_DEFAULT = originalExpirationDefault;
    }

    @Test
    public void testClose() throws Exception {
        suppress(method(CollectContextService.class, "onCacheRemoval"));

        CollectContextService collectContextService = new CollectContextService();
        UUID jobId = UUID.randomUUID();
        int shardId = 0;
        int jobSearchContextId = 1;

        // create context
        CrateSearchContext ctx1 = collectContextService.getOrCreateContext(jobId, shardId, jobSearchContextId, CONTEXT_FUNCTION);

        // sub-sequent call get existing context
        CrateSearchContext ctx2 = collectContextService.getOrCreateContext(jobId, shardId, jobSearchContextId, CONTEXT_FUNCTION);
        assertEquals(ctx1, ctx2);

        // invalidate
        collectContextService.close();

        // another call creates new context, because old one was deleted
        CrateSearchContext ctx3 = collectContextService.getOrCreateContext(jobId, shardId, jobSearchContextId, CONTEXT_FUNCTION);
        assertNotEquals(ctx1, ctx3);
    }
}
