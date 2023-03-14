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

import org.apache.ignite.deployment.DeploymentInfo;
import org.apache.ignite.deployment.UnitStatus;
import org.apache.ignite.deployment.UnitStatus.UnitStatusBuilder;
import org.apache.ignite.internal.deployunit.UnitMeta;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.key.UnitMetaSerializer;
import org.apache.ignite.internal.metastorage.Entry;

/**
 * Unit status accumulator.
 */
public class UnitStatusAccumulator implements Accumulator<UnitStatus> {
    private final String id;

    private UnitStatusBuilder builder;

    /**
     * Constructor.
     *
     * @param id Identifier of required unit.
     */
    public UnitStatusAccumulator(String id) {
        this.id = id;
    }

    @Override
    public void accumulate(Entry item) {
        if (builder == null) {
            builder = UnitStatus.builder(id);
        }
        UnitMeta meta = UnitMetaSerializer.deserialize(item.value());
        builder.append(meta.version(), new DeploymentInfo(meta.status(), meta.consistentIdLocation()));
    }

    @Override
    public UnitStatus get() throws AccumulateException {
        if (builder != null) {
            return builder.build();
        } else {
            throw new AccumulateException(
                    new DeploymentUnitNotFoundException("Unit with " + id + " doesn't exist."));
        }
    }
}
