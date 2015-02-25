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

package io.crate.analyze;

import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;

import java.util.List;

public class OrderBy {

    private List<Symbol> orderBySymbols;
    private boolean[] reverseFlags;
    private Boolean[] nullsFirst;

    public OrderBy(List<Symbol> orderBySymbols, boolean[] reverseFlags, Boolean[] nullsFirst) {
        assert orderBySymbols.size() == reverseFlags.length && reverseFlags.length == nullsFirst.length :
                "size of symbols / reverseFlags / nullsFirst must match";

        this.orderBySymbols = orderBySymbols;
        this.reverseFlags = reverseFlags;
        this.nullsFirst = nullsFirst;
    }

    public List<Symbol> orderBySymbols() {
        return orderBySymbols;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    public boolean isSorted() {
        return !orderBySymbols.isEmpty();
    }

    public void normalize(EvaluatingNormalizer normalizer) {
        normalizer.normalizeInplace(orderBySymbols);
    }

    public boolean hasFunction() {
        SortSymbolVisitor sortSymbolVisitor = new SortSymbolVisitor();
        SortSymbolContext ctx = new SortSymbolContext();
        for( Symbol symbol : this.orderBySymbols()) {
            sortSymbolVisitor.process(symbol, ctx);
            if(ctx.hasFunction) {
                return true;
            }
        }
        return false;
    }

    private static class SortSymbolContext {
        public boolean hasFunction = false;
    }

    private static class SortSymbolVisitor extends SymbolVisitor<SortSymbolContext, Void> {

        @Override
        public Void visitFunction(Function symbol, SortSymbolContext context) {
            context.hasFunction = true;
            return super.visitFunction(symbol, context);
        }
    }

}
