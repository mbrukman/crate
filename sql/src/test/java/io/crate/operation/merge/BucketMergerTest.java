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

package io.crate.operation.merge;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.CollectionBucket;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@SuppressWarnings("unchecked")
public class BucketMergerTest {

    private List<ListenableFuture<Bucket>> createBucketFutures(List<Object[]> ... buckets) {
        List<ListenableFuture<Bucket>> result = new ArrayList<>();
        for (List<Object[]> bucket : buckets) {
            Bucket realBucket = new CollectionBucket(bucket);
            result.add(Futures.immediateFuture(realBucket));
        }
        return result;
    }

    private void assertRows(Object[][] rows, String[] expected) {
        assertThat(TestingHelpers.printedTable(rows), is(Joiner.on("\n").join(expected) + "\n"));
    }

    @Test
    public void testMergeWith3Buckets() throws Exception {
        // page1
        Bucket p1Bucket1 = new CollectionBucket(Arrays.asList(
                new Object[] { "B" },
                new Object[] { "B" }
        ));
        Bucket p1Bucket2 = new CollectionBucket(Arrays.asList(
                new Object[] { "A" },
                new Object[] { "C" }
        ));
        Bucket p1Bucket3 = new CollectionBucket(Arrays.asList(
                new Object[] { "A" },
                new Object[] { "A" },
                new Object[] { "B" }
        ));
        List<ListenableFuture<Bucket>> p1Buckets = new ArrayList<>();
        p1Buckets.add(Futures.immediateFuture(p1Bucket1));
        p1Buckets.add(Futures.immediateFuture(p1Bucket2));
        p1Buckets.add(Futures.immediateFuture(p1Bucket3));

        CollectingProjector collectingProjector = new CollectingProjector();
        BucketMerger merger = new BucketMerger(
                collectingProjector, 0, 11, new int[] { 0 }, new boolean[] { false }, new Boolean[] { null });
        merger.merge(p1Buckets);
        merger.finish();
        Object[][] rows = collectingProjector.result().get();
        String[] expected = new String[] {"A", "A", "A", "B", "B", "B", "C"};
        assertRows(rows, expected);
    }

    @Test
    public void testMerge() throws Exception {
        // page1
        Bucket p1Bucket1 = new CollectionBucket(Arrays.asList(
                new Object[] { "A" },
                new Object[] { "A" },
                new Object[] { "B" },
                new Object[] { "C" }
        ));
        Bucket p1Bucket2 = new CollectionBucket(Arrays.asList(
                new Object[] { "B" },
                new Object[] { "B" }
        ));
        // page2
        Bucket p2Bucket1 = new CollectionBucket(Arrays.asList(
                new Object[] { "C" },
                new Object[] { "C" },
                new Object[] { "D" }
        ));
        Bucket p2Bucket2 = new CollectionBucket(Arrays.asList(
                new Object[] { "B" },
                new Object[] { "B" },
                new Object[] { "D" }
        ));

        List<ListenableFuture<Bucket>> p1Buckets = new ArrayList<>();
        p1Buckets.add(Futures.immediateFuture(p1Bucket1));
        p1Buckets.add(Futures.immediateFuture(p1Bucket2));

        List<ListenableFuture<Bucket>> p2Buckets = new ArrayList<>();
        p2Buckets.add(Futures.immediateFuture(p2Bucket1));
        p2Buckets.add(Futures.immediateFuture(p2Bucket2));

        CollectingProjector collectingProjector = new CollectingProjector();
        BucketMerger merger = new BucketMerger(
                collectingProjector, 0, 11, new int[] { 0 }, new boolean[] { false }, new Boolean[] { null });

        merger.merge(p1Buckets);
        merger.merge(p2Buckets);

        merger.finish();

        Object[][] rows = collectingProjector.result().get();
        String[] expected = new String[] {"A", "A", "B", "B", "B", "B", "B", "C", "C", "C", "D" };
        assertRows(rows, expected);
    }

    @Test
    public void testAllBucketsAreEmpty() throws Exception {
        List<ListenableFuture<Bucket>> p1Buckets = createBucketFutures(
                Arrays.<Object[]>asList(),
                Arrays.<Object[]>asList()
        );
        CollectingProjector collectingProjector = new CollectingProjector();
        BucketMerger merger = new BucketMerger(
                collectingProjector, 0, 11, new int[] { 0 }, new boolean[] { false }, new Boolean[] { null });
        merger.merge(p1Buckets);
        merger.finish();
        Object[][] rows = collectingProjector.result().get();
        assertThat(rows.length, is(0));
    }

    @Test
    public void testWithOnlyOneBucket() throws Exception {
        List<ListenableFuture<Bucket>> p1Buckets = createBucketFutures(
                Arrays.asList(
                        new Object[] { "A" },
                        new Object[] { "B" },
                        new Object[] { "C" }));
        List<ListenableFuture<Bucket>> p2Buckets = createBucketFutures(
                Arrays.asList(
                        new Object[] { "C" },
                        new Object[] { "C" },
                        new Object[] { "D" }));
        CollectingProjector collectingProjector = new CollectingProjector();
        BucketMerger merger = new BucketMerger(
                collectingProjector, 0, 11, new int[] { 0 }, new boolean[] { false }, new Boolean[] { null });
        merger.merge(p1Buckets);
        merger.merge(p2Buckets);
        merger.finish();

        Object[][] rows = collectingProjector.result().get();
        assertThat(rows.length, is(6));
        String[] expected = new String[] {"A", "B", "C", "C", "C", "D"};
        assertRows(rows, expected);
    }

    @Test
    public void testWithOneBucketOffsetAndLimit() throws Exception {
        List<ListenableFuture<Bucket>> p1Buckets = createBucketFutures(
                Arrays.asList(
                        new Object[] { "A" },
                        new Object[] { "B" },
                        new Object[] { "C" },
                        new Object[] { "D" }
                ));

        CollectingProjector collectingProjector = new CollectingProjector();
        BucketMerger merger = new BucketMerger(
                collectingProjector, 1, 2, new int[] { 0 }, new boolean[] { false }, new Boolean[] { null });
        merger.merge(p1Buckets);
        merger.finish();

        Object[][] rows = collectingProjector.result().get();
        assertThat(rows.length, is(2));
        String[] expected = new String[] {"B", "C" };
        assertRows(rows, expected);
    }

    @Test
    public void testWithTwoBucketsButOneIsEmpty() throws Exception {
    }
}