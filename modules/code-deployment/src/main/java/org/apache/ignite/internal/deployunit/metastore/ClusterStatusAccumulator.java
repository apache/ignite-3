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

import org.apache.ignite.internal.deployunit.UnitStatus;
import org.apache.ignite.internal.deployunit.UnitStatuses;
import org.apache.ignite.internal.deployunit.UnitStatuses.UnitStatusBuilder;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.metastore.key.UnitMetaSerializer;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.util.subscription.AccumulateException;
import org.apache.ignite.internal.util.subscription.Accumulator;

/**
 * Unit status accumulator.
 */
public class ClusterStatusAccumulator implements Accumulator<Entry, UnitStatuses> {
    private final String id;

    private UnitStatusBuilder builder;

    /**
     * Constructor.
     *
     * @param id Identifier of required unit.
     */
    public ClusterStatusAccumulator(String id) {
        this.id = id;
    }

    @Override
    public void accumulate(Entry item) {
        if (builder == null) {
            builder = UnitStatuses.builder(id);
        }
        UnitStatus meta = UnitMetaSerializer.deserialize(item.value());
        builder.append(meta.version(), meta.status()).build();
    }

    @Override
    public UnitStatuses get() throws AccumulateException {
        if (builder != null) {
            return builder.build();
        } else {
            throw new AccumulateException(
                    new DeploymentUnitNotFoundException("Unit with " + id + " doesn't exist."));
        }
    }
}
