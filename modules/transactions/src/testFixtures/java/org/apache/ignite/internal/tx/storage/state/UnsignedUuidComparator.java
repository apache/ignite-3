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

package org.apache.ignite.internal.tx.storage.state;

import java.util.Comparator;
import java.util.UUID;

/**
 * {@link Comparator} for {@link UUID} instances that orders them interpreting them as unsigned 128-bit integers.
 */
public class UnsignedUuidComparator implements Comparator<UUID> {
    @Override
    public int compare(UUID o1, UUID o2) {
        int highHalvesComparisonResult = Long.compareUnsigned(o1.getMostSignificantBits(), o2.getMostSignificantBits());

        if (highHalvesComparisonResult != 0) {
            return highHalvesComparisonResult;
        }

        return Long.compareUnsigned(o1.getLeastSignificantBits(), o2.getLeastSignificantBits());
    }
}
