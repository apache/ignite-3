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
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.util.subscription.AccumulateException;
import org.apache.ignite.internal.util.subscription.Accumulator;

/**
 * Units accumulator with filtering mechanism.
 */
public class ClusterStatusAccumulator implements Accumulator<Entry, List<UnitClusterStatus>> {

    private final List<UnitClusterStatus> result = new ArrayList<>();

    private final Predicate<UnitClusterStatus> filter;

    public ClusterStatusAccumulator() {
        this(t -> true);
    }

    public ClusterStatusAccumulator(Predicate<UnitClusterStatus> filter) {
        this.filter = filter;
    }

    @Override
    public void accumulate(Entry item) {
        byte[] value = item.value();
        if (value == null) {
            return;
        }

        UnitClusterStatus status = UnitClusterStatus.deserialize(value);
        if (filter.test(status)) {
            result.add(status);
        }
    }

    @Override
    public List<UnitClusterStatus> get() throws AccumulateException {
        return result;
    }
}
