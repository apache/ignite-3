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

package org.apache.ignite.internal.deployunit.metastore.accumulator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.util.subscription.AccumulateException;
import org.apache.ignite.internal.util.subscription.Accumulator;

/**
 * Node status accumulator with filter mechanism.
 */
public class NodeStatusAccumulator implements Accumulator<Entry, List<UnitNodeStatus>> {
    private final List<UnitNodeStatus> nodes = new ArrayList<>();

    private final Predicate<UnitNodeStatus> filter;

    public NodeStatusAccumulator() {
        this(status -> true);
    }

    public NodeStatusAccumulator(Predicate<UnitNodeStatus> filter) {
        this.filter = filter;
    }

    @Override
    public void accumulate(Entry item) {
        byte[] value = item.value();
        if (value != null) {
            UnitNodeStatus status = UnitNodeStatus.deserialize(value);
            if (filter.test(status)) {
                nodes.add(status);
            }
        }
    }

    @Override
    public List<UnitNodeStatus> get() throws AccumulateException {
        return nodes;
    }
}
