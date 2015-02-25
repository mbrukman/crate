/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.projectors;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row;
import io.crate.operation.ProjectorUpstream;

import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A projector which simply collects any rows it gets and makes them available afterwards.
 * Downstreams are not supported with this projector. This class should only be used as a fallback if
 * there are no other projectors in a chain but the result needs to be fetched at once after all
 * upstreams provided their rows.
 */
public class CollectingProjector implements ResultProvider, Projector {

    private final AtomicInteger upstreamsRemaining;
    public final Vector<Object[]> rows = new Vector<>();
    private SettableFuture<Bucket> result = SettableFuture.create();


    public CollectingProjector() {
        this.upstreamsRemaining = new AtomicInteger(0);
    }

    @Override
    public void startProjection() {
        if (upstreamsRemaining.get() <= 0) {
            upstreamFinished();
        }
    }

    @Override
    public boolean setNextRow(Row row) {
        Object[] materialized = new Object[row.size()];
        for (int i = 0; i < materialized.length; i++) {
            materialized[i] = row.get(i);
        }
        rows.add(materialized);
        return true;
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        upstreamsRemaining.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        if (upstreamsRemaining.decrementAndGet() <= 0) {
            result.set(new CollectionBucket(rows));
        }
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        if (upstreamsRemaining.decrementAndGet() <= 0) {
            result.setException(throwable);
        }
    }

    @Override
    public ListenableFuture<Bucket> result() {
        return result;
    }

}
