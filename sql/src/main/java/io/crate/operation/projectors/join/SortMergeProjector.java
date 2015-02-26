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

package io.crate.operation.projectors.join;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.Projector;
import io.crate.operation.projectors.YProjector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO: move to Row interface
 */
public class SortMergeProjector implements YProjector, ProjectorUpstream {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final Projector leftProjector;
    private final Projector rightProjector;

    private Projector downstream;
    private final int offset;
    private final int limit;
    private final AtomicBoolean wantMore;
    private final AtomicInteger rowsSkipped;
    private final AtomicInteger rowsProduced;
    private final AtomicInteger activeProjectors;

    private final CollectExpression[] leftCollectExpressions;
    private final CollectExpression[] rightCollectExpressions;

    private final Ordering[] comparators;

    private final BlockingQueue<List<Object[]>> currentRightRows;
    private final BlockingQueue<List<Object[]>> currentLeftRows;
    private final AtomicBoolean projectionStarted;

    public SortMergeProjector(int offset,
                              int limit,
                              CollectExpression[] leftCollectExpressions,
                              CollectExpression[] rightCollectExpressions,
                              Ordering[] comparators) {
        Preconditions.checkArgument(leftCollectExpressions.length == rightCollectExpressions.length,
                "number of join attributes on each side differ");
        Preconditions.checkArgument(leftCollectExpressions.length == comparators.length,
                "number of comparators differs from join attributes");
        this.offset = offset;
        this.limit = limit;
        this.comparators = comparators;
        this.rowsSkipped = new AtomicInteger(0);
        this.rowsProduced = new AtomicInteger(0);
        this.wantMore = new AtomicBoolean(true);
        this.activeProjectors = new AtomicInteger(2);
        this.leftCollectExpressions = leftCollectExpressions;
        this.rightCollectExpressions = rightCollectExpressions;
        this.currentRightRows = new ArrayBlockingQueue<>(1);
        this.currentLeftRows = new ArrayBlockingQueue<>(1);
        this.leftProjector = new InternalProjector() {
            @Override
            boolean doSetNextRows(List<Object[]> rows) {
                return setNextLeftRows(rows);
            }
        };
        this.rightProjector = new InternalProjector() {
            @Override
            boolean doSetNextRows(List<Object[]> rows) {
                return setNextRightRows(rows);
            }
        };
        this.projectionStarted = new AtomicBoolean(false);
    }

    private void onProjectorFinished() {
        if (activeProjectors.decrementAndGet() == 0 && downstream != null) {
            downstream.upstreamFinished();
        }
    }

    private void onProjectorFailed(Throwable throwable) {
        if (downstream != null) {
            downstream.upstreamFailed(throwable);
        }
    }

    @Override
    public void startProjection() {
        if (!projectionStarted.getAndSet(true)) {
            downstream.startProjection();
        }
    }

    @SuppressWarnings("unchecked")
    private int compare(Object[] left, Object[] right) {
        for (CollectExpression leftCollectExpression : leftCollectExpressions) {
            leftCollectExpression.setNextRow(left);
        }
        for (CollectExpression rightCollectExpression : rightCollectExpressions) {
            rightCollectExpression.setNextRow(right);
        }
        int comparisonResult = 0;
        for (int i = 0, size = Math.max(rightCollectExpressions.length, leftCollectExpressions.length); i < size; i++) {

            comparisonResult = comparators[i].compare(
                    leftCollectExpressions[i].value(),
                    rightCollectExpressions[i].value()
            );
            if (comparisonResult != 0) {
                return comparisonResult;
            }
        }
        return comparisonResult;
    }

    @Override
    public Projector leftProjector() {
        return leftProjector;
    }

    @Override
    public Projector rightProjector() {
        return rightProjector;
    }

    private boolean setNextLeftRows(List<Object[]> leftRows) {
        try {
            logger.trace("get left rows {}", Arrays.deepToString(leftRows.toArray()));
            if (leftRows != null && !leftRows.isEmpty()) {
                this.currentLeftRows.put(leftRows);
                List<Object[]> rightRows = this.currentRightRows.peek();
                if (rightRows != null) {
                    consumeRows(leftRows, rightRows);
                }
            }
        } catch (InterruptedException e) {
            // TODO: really propagate?
            downstream.upstreamFailed(e);
            Thread.currentThread().interrupt();
        }
        return this.wantMore.get();
    }

    private boolean setNextRightRows(List<Object[]> rightRows) {
        try {
            logger.trace("get right rows {}", Arrays.deepToString(rightRows.toArray()));
            // ignore null or empty groups
            if (rightRows != null && !rightRows.isEmpty()) {
                this.currentRightRows.put(rightRows);
                List<Object[]> leftRows = this.currentLeftRows.peek();
                if (leftRows !=null) {
                    consumeRows(leftRows, rightRows);
                }
            }
        } catch (InterruptedException e) {
            // TODO: really propagate?
            downstream.upstreamFailed(e);
            Thread.currentThread().interrupt();
        }
        return this.wantMore.get();
    }

    private void consumeRows(List<Object[]> leftRows, List<Object[]> rightRows) {
        if (wantMore.get()) {
            int compared = compare(leftRows.get(0), rightRows.get(0));
            logger.trace("consume {}");
            if (compared < 0) {
                // left rows are smaller than right, skip to next left set
                currentLeftRows.poll();
            } else if (compared == 0) {
                // both groups have same join conditions
                // NESTEDLOOP FTW
                Outer:
                for (Object[] leftRow : leftRows) {
                    for (Object[] rightRow : rightRows) {
                        if (rowsSkipped.incrementAndGet() < offset) {
                            break Outer;
                        }
                        boolean downStreamWantsMore = downstream.setNextRow(RowCombinator.combineRow(leftRow, rightRow));
                        if (rowsProduced.incrementAndGet() >= limit || !downStreamWantsMore) {
                            wantMore.set(false);
                            break Outer;
                        }
                    }
                }
            } else {
                // right rows are smaller than left, skip to next right set
                currentRightRows.poll();
            }
        }
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    private abstract class InternalProjector implements Projector {

        private final List<Object[]> sameValueRows;
        private final AtomicInteger remainingUpstreams;

        private InternalProjector() {
            this.remainingUpstreams = new AtomicInteger(0);
            this.sameValueRows = new LinkedList<>();
        }

        @Override
        public boolean setNextRow(Object... row) {
            boolean wantMore = true;
            if (sameValueRows.isEmpty()) {
                sameValueRows.add(row);
            } else {
                Object[] lastRow = sameValueRows.get(0);
                int result = compare(lastRow, row);
                if (result < 0) {
                    // TODO: optimize to not copy
                    wantMore = doSetNextRows(
                            ImmutableList.copyOf(sameValueRows)
                    );
                    sameValueRows.clear();
                } else if (result == 0) {
                    sameValueRows.add(row);
                } else {
                    throw new IllegalStateException("source not ordered");
                }
            }
            return wantMore;
        }

        abstract boolean doSetNextRows(List<Object[]> row);

        @Override
        public void startProjection() {
            SortMergeProjector.this.startProjection();
        }

        @Override
        public void registerUpstream(ProjectorUpstream upstream) {
            remainingUpstreams.incrementAndGet();
            upstream.downstream(this);
        }

        @Override
        public void upstreamFinished() {
            if (remainingUpstreams.decrementAndGet() == 0) {
                onProjectorFinished();
            }
        }

        @Override
        public void upstreamFailed(Throwable throwable) {
            onProjectorFailed(throwable);
        }
    }
}
