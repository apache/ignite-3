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

package org.apache.ignite.internal.sql.engine.trait;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.HashFunctionFactory.RowHashFunction;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Partitioned.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public final class Partitioned<RowT> implements Destination<RowT> {
    private final List<List<String>> assignments;

    private final RowHashFunction<RowT> partFun;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public Partitioned(List<String> assignments, RowHashFunction<RowT> partFun) {
        this.assignments = Commons.transform(assignments, List::of);
        this.partFun = partFun;
    }

    /** {@inheritDoc} */
    @Override
    public List<String> targets(RowT row) {
        return assignments.get(IgniteUtils.safeAbs(partFun.hashOf(row) % assignments.size()));
    }

    /** {@inheritDoc} */
    @Override
    public List<String> targets() {
        return assignments.stream()
                .flatMap(Collection::stream)
                .distinct().collect(Collectors.toList());
    }
}
