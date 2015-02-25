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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.projectors.Projector;
import io.crate.operation.projectors.sorting.OrderingByPosition;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BucketMerger {

    private final Projector downstream;
    private final Ordering<Row> ordering;
    private final int offset;
    private final int limit;
    private final AtomicInteger rowsEmitted;
    private final AtomicInteger rowsSkipped;
    private Iterator<Row>[] remainingBucketIts = null;

    public BucketMerger(Projector downstream,
                        int offset,
                        int limit,
                        int[] orderByPositions,
                        boolean[] reverseFlags,
                        Boolean[] nullsFirst) {
        this.offset = offset;
        this.limit = limit;
        List<Comparator<Row>> comparators = new ArrayList<>(orderByPositions.length);
        for (int i = 0; i < orderByPositions.length; i++) {
            comparators.add(new OrderingByPosition(i, reverseFlags[i], nullsFirst[i]));
        }
        ordering = Ordering.compound(comparators);
        this.downstream = downstream;
        rowsEmitted = new AtomicInteger(0);
        rowsSkipped = new AtomicInteger(0);
    }

    /**
     * This is basically a sort-merge-join but with more than 2 sides.
     * Results must be pre-sorted for this to work
     *
     * p1
     * b1: [ A , A, B, C ]
     * b2: [ B, B ]
     *
     * p2
     * b1: [ C, C, D ]
     * b2: [ B, B, D ]
     *
     * output:
     *  [ A, A, B, B, B, B, C, C, C, D, D ]
     *
     * see private emitBuckets(...) for more details on how this works
     */
    public void merge(List<ListenableFuture<Bucket>> buckets) {
        final AtomicReference<Throwable> throwable = new AtomicReference<>();
        Futures.addCallback(Futures.allAsList(buckets), new FutureCallback<List<Bucket>>() {
            @Override
            public void onSuccess(List<Bucket> result) {
                try {
                    emitBuckets(result);
                } catch (Throwable t) {
                    throwable.set(t);
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                downstream.upstreamFailed(t);
            }
        });
        Throwable t = throwable.get();
        if (t != null) {
            throw Throwables.propagate(t);
        }
        // TODO: block here?
    }

    private void emitBuckets(List<Bucket> buckets) {
        ArrayList<Iterator<Row>> bucketIts = getIterators(buckets);
        emitBuckets(bucketIts);
    }

    /**
     * multi bucket sort-merge based on iterators
     *
     * Page1  (first merge call)
     *     B1      B2       B3
     *
     *    ->B     ->A     ->A
     *      B       C       A
     *                      B
     *
     * first iteration across all buckets:
     *
     *      leastRow:                   A (from b2)
     *      equal (or also leastRow):   b3
     *
     *      these will be emitted
     *
     * second iteration:
     *
     *    ->B       A       A
     *      B     ->C     ->A
     *                      B
     *
     *      leastRow:   A (from b3)
     *      equal: none
     *
     * third iteration:
     *
     *    ->B       A       A
     *      B     ->C       A
     *                    ->B
     *
     *      leastRow:   B (from b1)
     *      equal:      B (from b2)
     *
     * fourth iteration:
     *
     *      B       A       A
     *    ->B     ->C       A
     *                      B
     *
     *      leastRow:   B (from b1)
     *
     * after the fourth iteration the iterator that had the leastRow (B1) will be exhausted
     * which causes B2 (Row C) to be put into the remainingIterators which will be used if a new merge call is made
     */
    private void emitBuckets(ArrayList<Iterator<Row>> bucketIts) {
        if (bucketIts.isEmpty()) { // all buckets are empty. Yay nothing to do!
            // TODO: wait for finish call before calling upstreamFinished?
            downstream.upstreamFinished();
            return;
        } else if (bucketIts.size() == 1) {
            emitSingleBucket(bucketIts.get(0));
            return;
        }
        List<Row> previousRows = new ArrayList<>(bucketIts.size());
        for (Iterator<Row> bucketIt : bucketIts) {
            if (bucketIt.hasNext()) {
                previousRows.add(bucketIt.next());
            } else {
                previousRows.add(null);
            }
        }
        final int numBuckets = bucketIts.size();

        int bi = 0;
        Row leastRow = null;
        int leastBi = -1;
        IntArrayList bucketsWithRowEqualToLeast = new IntArrayList(numBuckets);
        IntOpenHashSet exhaustedIterators = new IntOpenHashSet(numBuckets, 1);

        while (exhaustedIterators.size() < numBuckets) {

            Row row = previousRows.get(bi);
            if (row == null) {
                exhaustedIterators.add(bi);
            }

            if (leastRow == null) {
                // TODO: use one spare row for leastRow instead of copying each time
                leastRow = copyRow(row);
                leastBi = bi;
                bi = bi < numBuckets - 1 ? bi + 1 : 0;
            } else {
                int compare = ordering.compare(leastRow, row);
                if (compare < 0) {
                    leastBi = bi;
                    // TODO: use one spare row for leastRow instead of copying each time
                    leastRow = copyRow(row);
                } else if (compare == 0) {
                    bucketsWithRowEqualToLeast.add(bi);
                }

                bi++;
                if (bi == numBuckets) {
                    // looked at all buckets..
                    boolean leastBucketItExhausted = false;

                    emit(leastRow);
                    Iterator<Row> bucketItWithLeastRow = bucketIts.get(leastBi);
                    if (bucketItWithLeastRow.hasNext()) {
                        previousRows.set(leastBi, bucketItWithLeastRow.next());
                    } else {
                        previousRows.set(leastBi, null);
                        leastBucketItExhausted = true;
                    }

                    // send for all other buckets that are equal to least
                    for (IntCursor equalBucketIdx : bucketsWithRowEqualToLeast) {
                        emit(leastRow);
                        Iterator<Row> equalBucketIt = bucketIts.get(equalBucketIdx.value);
                        if (equalBucketIt.hasNext()) {
                            previousRows.set(equalBucketIdx.value, equalBucketIt.next());
                        } else {
                            previousRows.set(equalBucketIdx.value, null);
                            exhaustedIterators.add(bi);
                        }
                    }
                    leastRow = null;
                    bucketsWithRowEqualToLeast.clear();
                    bi = 0;

                    if (leastBucketItExhausted) {
                        // need next page to continue...
                        for (int i = 0; i < numBuckets; i++) {
                            Iterator<Row> bucketIt = bucketIts.get(i);
                            Row previousRow = previousRows.get(i);
                            if (previousRow != null) {
                                Iterator<Row> iterator = ImmutableList.of(previousRow).iterator();
                                if (bucketIt.hasNext()) {
                                    iterator = Iterators.concat(iterator, bucketIt);
                                }
                                remainingBucketIts[i] = iterator;
                            } else if (bucketIt.hasNext()) {
                                remainingBucketIts[i] = bucketIt;
                            }
                        }
                        return;
                    }
                }
            }
        }
    }

    private ArrayList<Iterator<Row>> getIterators(List<Bucket> buckets) {
        ArrayList<Iterator<Row>> bucketIts = new ArrayList<>(buckets.size());
        if (remainingBucketIts == null) {
            //noinspection unchecked
            remainingBucketIts = new Iterator[buckets.size()];
            for (Bucket bucket : buckets) {
                bucketIts.add(bucket.iterator());
            }
        } else {
            assert buckets.size() == remainingBucketIts.length :
                    "number of buckets between merge calls must remain the same";
            for (int i = 0; i < buckets.size(); i++) {
                Iterator<Row> remainingBucketIt = remainingBucketIts[i];
                if (remainingBucketIt == null) {
                    bucketIts.add(buckets.get(i).iterator());
                } else {
                    bucketIts.add(Iterators.concat(remainingBucketIt, buckets.get(i).iterator()));
                }
            }
        }
        return bucketIts;
    }

    private void emitSingleBucket(Iterator<Row> rowIterator) {
        while (rowIterator.hasNext()) {
            if (!emit(rowIterator.next())) {
                return;
            }
        }
    }

    private boolean emit(Row row) {
        if (rowsSkipped.getAndIncrement() < offset) {
            return true;
        }
        if (rowsEmitted.getAndIncrement() < limit) {
            return downstream.setNextRow(rowToArray(row));
        }
        return false;
    }

    private Row copyRow(Row row) {
        if (row == null) {
            return null;
        }
        Object[] arr = new Object[row.size()];
        for (int i = 0; i < row.size(); i++) {
            arr[i] = row.get(i);
        }
        return new RowN(arr);
    }

    public void finish() {
        ArrayList<Iterator<Row>> bucketIts = new ArrayList<>(remainingBucketIts.length);
        for (Iterator<Row> bucketIt : remainingBucketIts) {
            if (bucketIt == null) {
                bucketIts.add(Collections.<Row>emptyIterator());
            } else {
                bucketIts.add(bucketIt);
            }
        }
        emitBuckets(bucketIts);
        downstream.upstreamFinished();
    }

    // TODO: remove once Projector is row based
    private Object[] rowToArray(Row row) {
        Object[] rowArray = new Object[row.size()];
        for (int j = 0; j < row.size(); j++) {
            rowArray[j] = row.get(j);
        }
        return rowArray;
    }
}
