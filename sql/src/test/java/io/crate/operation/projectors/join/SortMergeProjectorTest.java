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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.projectors.Projector;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public class SortMergeProjectorTest extends RandomizedTest {

    private Object[][] toOneColRows(Object ... values) {
        return toRows(1, values);
    }

    private Object[][] toRows(int numColumns, Object ... values) {
        int size = values.length / numColumns;
        Object[][] rows = new Object[size][];
        Iterator<Object> iter = Iterators.forArray(values);
        for (int i = 0; i < size; i++) {
            Object[] row = new Object[numColumns];
            for (int j = 0; j < numColumns; j++) {
                row[j] = iter.hasNext() ? iter.next() : null;
            }
            rows[i] = row;
        }
        return rows;
    }

    private static class UpstreamRunnable implements Runnable, ProjectorUpstream {

        private final Object[][] rows;
        private Projector downstream;

        private UpstreamRunnable(Object[][] rows) {
            this.rows = rows;
        }

        @Override
        public void downstream(Projector downstream) {
           this.downstream = downstream;
        }

        @Override
        public void run() {
            downstream.startProjection();
            for (Object[] row : rows) {
                downstream.setNextRow(row);
            }
            downstream.upstreamFinished();
        }
    }

    @Test
    public void testFromDifferentThreads() throws Exception {
        final CollectExpression[] leftCollectExpressions = new CollectExpression[] { new InputCollectExpression(0) };
        CollectExpression[] rightCollectExpressions = new CollectExpression[] { new InputCollectExpression(0) };
        Ordering[] comparators = new Ordering[] {Ordering.natural().nullsFirst()};
        SortMergeProjector sortMergeProjector = new SortMergeProjector(0, 10,
                leftCollectExpressions,
                rightCollectExpressions,
                comparators
        );
        CollectingProjector collectingProjector = new CollectingProjector();
        sortMergeProjector.downstream(collectingProjector);

        Projector leftProjector = sortMergeProjector.leftProjector();
        Projector rightProjector = sortMergeProjector.rightProjector();
        Object[][] leftRows = toOneColRows(0, 0, 1, 2, 2);
        Object[][] rightRows = toOneColRows(1, 1, 3);

        UpstreamRunnable leftRunnable = new UpstreamRunnable(leftRows);
        leftProjector.registerUpstream(leftRunnable);
        UpstreamRunnable rightRunnable = new UpstreamRunnable(rightRows);
        rightProjector.registerUpstream(rightRunnable);

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(leftRunnable);
        executorService.submit(rightRunnable);
        executorService.shutdown();
        assertThat(
                executorService.awaitTermination(10, TimeUnit.MINUTES),
                is(true)
        );
        executorService.shutdownNow();

        assertThat(
                TestingHelpers.printedTable(collectingProjector.result().get()),
                is("1|1\n 1|1\n 1|1\n 1|1\n")

        );
    }
}
