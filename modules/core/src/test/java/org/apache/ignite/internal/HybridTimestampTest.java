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

package org.apache.ignite.internal.hlc;

import static org.apache.ignite.internal.hlc.HybridTimestamp.max;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Tests of a hybrid timestamp implementation.
 * {@link HybridTimestamp}
 */
class HybridTimestampTest {
    @Test
    public void testComparison() {
        assertEquals(new HybridTimestamp(10, 5),
                max(new HybridTimestamp(10, 5), new HybridTimestamp(5, 7))
        );

        assertEquals(new HybridTimestamp(20, 10),
                max(new HybridTimestamp(10, 100), new HybridTimestamp(20, 10))
        );

        assertEquals(new HybridTimestamp(20, 10),
                max(new HybridTimestamp(20, 10))
        );

        assertEquals(null, max());
    }
}
