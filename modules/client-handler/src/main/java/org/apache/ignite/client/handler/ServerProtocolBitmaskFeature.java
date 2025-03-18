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

package org.apache.ignite.client.handler;

import java.util.BitSet;
import org.apache.ignite.table.QualifiedName;

/**
 * Defines supported bitmask features.
 */
public enum ServerProtocolBitmaskFeature {
    /**
     * TABLE_GET/TABLES_GET requests with {@link QualifiedName}.
     */
    TABLE_GET_REQS_USE_QUALIFIED_NAME(1);

    private final int featureId;

    ServerProtocolBitmaskFeature(int featureId) {
        this.featureId = featureId;
    }

    /** Feature id. */
    public int featureId() {
        return featureId;
    }

    /** Converts the given collection of features into a bit set. */
    public static BitSet toBitSet(ServerProtocolBitmaskFeature... features) {
        BitSet bitSet = new BitSet();
        for (var f : features) {
            bitSet.set(f.featureId);
        }
        return bitSet;
    }
}
