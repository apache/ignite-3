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

package org.apache.ignite.internal.client.proto;

import java.util.BitSet;
import java.util.Collection;
import java.util.EnumSet;
import org.apache.ignite.table.QualifiedName;

/**
 * Defines supported bitmask features for thin client.
 */
public enum ProtocolBitmaskFeature {
    /** Feature for user attributes. */
    USER_ATTRIBUTES(0),

    /**
     * TABLE_GET/TABLES_GET requests with {@link QualifiedName}.
     */
    TABLE_GET_REQS_USE_QUALIFIED_NAME(1);

    private static final EnumSet<ProtocolBitmaskFeature> ALL_FEATURES_AS_ENUM_SET =
            EnumSet.allOf(ProtocolBitmaskFeature.class);

    /** Feature id. */
    private final int featureId;

    /**
     * Constructor.
     *
     * @param id Feature ID.
     */
    ProtocolBitmaskFeature(int id) {
        featureId = id;
    }

    /**
     * Returns feature ID.
     *
     * @return Feature ID.
     */
    public int featureId() {
        return featureId;
    }

    /**
     * Returns set of supported features.
     *
     * @param bitSet Features bitset.
     * @return Set of supported features.
     */
    public static EnumSet<ProtocolBitmaskFeature> enumSet(BitSet bitSet) {
        EnumSet<ProtocolBitmaskFeature> set = EnumSet.noneOf(ProtocolBitmaskFeature.class);

        for (ProtocolBitmaskFeature e : values()) {
            if (bitSet.get(e.featureId())) {
                set.add(e);
            }
        }

        return set;
    }

    /**
     * Returns byte array representing all supported features.
     *
     * @param features Feature set.
     * @return Byte array representing all supported features.
     */
    public static BitSet featuresAsBitSet(Collection<ProtocolBitmaskFeature> features) {
        BitSet set = new BitSet();

        for (ProtocolBitmaskFeature f : features) {
            set.set(f.featureId());
        }

        return set;
    }

    /**
     * Returns all features as a set.
     *
     * @return All features as a set.
     */
    public static EnumSet<ProtocolBitmaskFeature> allFeaturesAsEnumSet() {
        return ALL_FEATURES_AS_ENUM_SET.clone();
    }
}
