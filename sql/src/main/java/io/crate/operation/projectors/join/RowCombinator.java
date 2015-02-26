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

import io.crate.core.collections.Row;

public class RowCombinator {

    public static class CombinedRow implements Row {

        private final Row left;
        private final Row right;

        public CombinedRow(Row left, Row right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public int size() {
            return left.size() + right.size();
        }

        @Override
        public Object get(int index) {
            if (index >= left.size()) {
                return right.get(index - left.size());
            } else {
                return left.get(index);
            }
        }
    }

    public static Row combineRow(Row innerRow, Row outerRow) {
        return new CombinedRow(innerRow, outerRow);
    }

    public static Object[] combineRow(Object[] innerRow, Object[] outerRow) {
        Object[] newRow = new Object[outerRow.length + innerRow.length];
        System.arraycopy(outerRow, 0, newRow, 0, outerRow.length);
        System.arraycopy(innerRow, 0, newRow, outerRow.length, innerRow.length);
        return newRow;
    }
}
