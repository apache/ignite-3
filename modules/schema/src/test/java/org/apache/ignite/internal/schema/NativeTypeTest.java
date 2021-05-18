/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema;

import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class NativeTypeTest {
    /**
     *
     */
    @Test
    public void testCompareFixlenTypesVsVarlenTypes() {
        assertTrue(NativeTypes.BYTE.compareTo(STRING) < 0);
        assertTrue(NativeTypes.BYTE.compareTo(BYTES) < 0);

        assertTrue(NativeTypes.INTEGER.compareTo(STRING) < 0);
        assertTrue(NativeTypes.INTEGER.compareTo(BYTES) < 0);

        assertTrue(NativeTypes.LONG.compareTo(STRING) < 0);
        assertTrue(NativeTypes.LONG.compareTo(BYTES) < 0);

        assertTrue(NativeTypes.UUID.compareTo(STRING) < 0);
        assertTrue(NativeTypes.UUID.compareTo(BYTES) < 0);
    }

    /**
     *
     */
    @Test
    public void testCompareFixlenTypesBySize() {
        assertTrue(NativeTypes.SHORT.compareTo(NativeTypes.INTEGER) < 0);
        assertTrue(NativeTypes.INTEGER.compareTo(NativeTypes.LONG) < 0);
        assertTrue(NativeTypes.LONG.compareTo(NativeTypes.UUID) < 0);
    }

    /**
     *
     */
    @Test
    public void testCompareFixlenTypesByDesc() {
        assertTrue(NativeTypes.FLOAT.compareTo(NativeTypes.INTEGER) < 0);
    }

    /**
     *
     */
    @Test
    public void testCompareVarlenTypesByDesc() {
        assertTrue(BYTES.compareTo(STRING) < 0);
    }
}
