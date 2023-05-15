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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.deployunit.UnitMeta;
import org.apache.ignite.internal.deployunit.metastore.key.UnitKey;
import org.apache.ignite.internal.deployunit.metastore.key.UnitMetaSerializer;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;
import org.apache.ignite.internal.util.subscription.AccumulateException;
import org.apache.ignite.internal.util.subscription.Accumulator;

/**
 * Units id accumulator for by node deployment.
 */
public class UnitsByNodeAccumulator implements Accumulator<Entry, List<String>> {
    private final String consistentId;

    private final List<String> result = new ArrayList<>();

    public UnitsByNodeAccumulator(String consistentId) {
        this.consistentId = consistentId;
    }

    @Override
    public void accumulate(Entry item) {
        byte[] key = item.key();
        byte[] value = item.value();
        String nodeId = UnitKey.extractNodeId(key);

        if (Objects.equals(nodeId, consistentId)) {
            UnitMeta meta = UnitMetaSerializer.deserialize(value);
            if (meta.status() == DeploymentStatus.DEPLOYED) {
                result.add(meta.id());
            }
        }
    }

    @Override
    public List<String> get() throws AccumulateException {
        return result;
    }
}
