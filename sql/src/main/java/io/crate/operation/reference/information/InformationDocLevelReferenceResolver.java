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

package io.crate.operation.reference.information;

import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.information.RowCollectExpression;
import io.crate.operation.reference.DocLevelReferenceResolver;
import org.elasticsearch.common.inject.Singleton;

import java.util.HashMap;
import java.util.Map;

@Singleton
public class InformationDocLevelReferenceResolver implements DocLevelReferenceResolver<RowCollectExpression<?, ?>> {

    public static final InformationDocLevelReferenceResolver INSTANCE = new InformationDocLevelReferenceResolver();

    private static final Map<ReferenceIdent, RowCollectExpression<?, ?>> implementations = new HashMap<>();

    static {
        for (RowCollectExpression<?, ?> implementation : InformationTablesExpression.IMPLEMENTATIONS) {
            implementations.put(implementation.info().ident(), implementation);
        }
        for (RowCollectExpression<?, ?> implementation : InformationTablePartitionsExpression.IMPLEMENTATIONS) {
            implementations.put(implementation.info().ident(), implementation);
        }
        for (RowCollectExpression<?, ?> implementation : InformationColumnsExpression.IMPLEMENTATIONS) {
            implementations.put(implementation.info().ident(), implementation);
        }
        for (RowCollectExpression<?, ?> implementation : InformationTableConstraintsExpression.IMPLEMENTATIONS) {
            implementations.put(implementation.info().ident(), implementation);
        }
        for (RowCollectExpression<?, ?> implementation : InformationRoutinesExpression.IMPLEMENTATIONS) {
            implementations.put(implementation.info().ident(), implementation);
        }
    }

    /**
     * This is a singleton Use the static INSTANCE attribute
     */
    private InformationDocLevelReferenceResolver() {

    }

    @Override
    public RowCollectExpression<?, ?> getImplementation(ReferenceInfo info) {
        return implementations.get(info.ident());
    }
}
