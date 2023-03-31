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

package org.apache.ignite.internal.deployunit.metastore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.deployunit.DeploymentInfo;
import org.apache.ignite.internal.deployunit.UnitMeta;
import org.apache.ignite.internal.deployunit.UnitStatus;
import org.apache.ignite.internal.deployunit.UnitStatus.UnitStatusBuilder;
import org.apache.ignite.internal.deployunit.key.UnitMetaSerializer;
import org.apache.ignite.internal.metastorage.Entry;

/**
 * Units accumulator with filtering mechanism.
 */
public class UnitsAccumulator implements Accumulator<List<UnitStatus>> {
    private final Map<String, UnitStatusBuilder> map = new HashMap<>();

    private final Predicate<UnitMeta> filter;

    public UnitsAccumulator() {
        this(t -> true);
    }

    public UnitsAccumulator(Predicate<UnitMeta> filter) {
        this.filter = filter;
    }

    @Override
    public void accumulate(Entry item) {
        UnitMeta meta = UnitMetaSerializer.deserialize(item.value());
        if (filter.test(meta)) {
            map.computeIfAbsent(meta.id(), UnitStatus::builder)
                    .append(meta.version(),
                            DeploymentInfo.builder()
                                    .status(meta.status())
                                    .addConsistentIds(meta.consistentIdLocation()).build()
                    );
        }
    }

    @Override
    public List<UnitStatus> get() throws AccumulateException {
        return map.values().stream().map(UnitStatusBuilder::build).collect(Collectors.toList());
    }
}
