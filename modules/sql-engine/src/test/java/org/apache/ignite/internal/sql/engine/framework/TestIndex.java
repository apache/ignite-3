/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.framework;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rel.RelCollation;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;

/**
 * A test index that implements all the necessary for the optimizer methods to be used to prepare a query.
 */
public class TestIndex extends IgniteIndex {
    /** Factory method for creating hash-index. */
    static TestIndex createHash(
            String name,
            List<String> columns,
            TableDescriptor tableDescriptor
    ) {
        RelCollation collation = TraitUtils.createCollation(columns, null, tableDescriptor);

        return new TestIndex(name, Type.HASH, tableDescriptor.distribution(), collation);
    }

    /** Factory method for creating sorted-index. */
    static TestIndex createSorted(
            String name,
            List<String> columns,
            List<Collation> collations,
            TableDescriptor tableDescriptor
    ) {
        RelCollation collation = TraitUtils.createCollation(columns, collations, tableDescriptor);

        return new TestIndex(name, Type.SORTED, tableDescriptor.distribution(), collation);
    }

    private static final AtomicInteger ID = new AtomicInteger();

    /** Constructor. */
    TestIndex(
            String name,
            Type type,
            IgniteDistribution distribution,
            RelCollation collation
    ) {
        super(ID.incrementAndGet(), name, type, distribution, collation);
    }
}
