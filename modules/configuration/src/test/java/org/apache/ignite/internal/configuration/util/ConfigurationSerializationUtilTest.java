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

package org.apache.ignite.internal.configuration.util;

import static org.apache.ignite.internal.configuration.util.ConfigurationSerializationUtil.fromBytes;
import static org.apache.ignite.internal.configuration.util.ConfigurationSerializationUtil.toBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ConfigurationSerializationUtilTest {
    @Test
    void testSingleValuesConsistency() {
        assertSingleValue(Boolean.FALSE);
        assertSingleValue(Boolean.TRUE);

        assertSingleValue((byte) 123);

        assertSingleValue((short) 0x1234);

        assertSingleValue(0x12345678);

        assertSingleValue(0x123456789ABCDEF0L);

        assertSingleValue('F');

        assertSingleValue(0.3f);

        assertSingleValue(0.3d);

        assertSingleValue("foo");

        assertSingleValue(UUID.randomUUID());
    }

    @Test
    void testArraysConsistency() {
        assertArray(new boolean[]{false, true});

        assertArray(new byte[]{10, -10});

        assertArray(new short[]{1000, -1000});

        assertArray(new int[]{1000_000, -1000_000});

        assertArray(new long[]{1000_000_000_000L, -1000_000_000_000L});

        assertArray(new char[]{'X', 'Y'});

        assertArray(new float[]{0.1f, -0.1f});

        assertArray(new double[]{0.1d, -0.1d});

        assertArray(new String[]{"foo", "bar"});

        assertArray(new UUID[]{UUID.randomUUID(), UUID.randomUUID()});
    }

    private void assertSingleValue(Object value) {
        assertEquals(value, fromBytes(toBytes(value)));
    }

    private void assertArray(Object value) {
        Serializable res = fromBytes(toBytes(value));

        assertEquals(value.getClass(), res.getClass());

        assertEquals(Array.getLength(value), Array.getLength(res));

        for (int i = 0; i < Array.getLength(value); i++) {
            assertEquals(Array.get(value, i), Array.get(res, i));
        }
    }
}
