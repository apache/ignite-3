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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class NativeTypeTest {
    /**
     *
     */
    @Test
    public void testCompareFixlenVarlen() {
        assertTrue(NativeType.BYTE.compareTo(NativeType.STRING) < 0);
        assertTrue(NativeType.BYTE.compareTo(NativeType.BYTES) < 0);

        assertTrue(NativeType.INTEGER.compareTo(NativeType.STRING) < 0);
        assertTrue(NativeType.INTEGER.compareTo(NativeType.BYTES) < 0);

        assertTrue(NativeType.LONG.compareTo(NativeType.STRING) < 0);
        assertTrue(NativeType.LONG.compareTo(NativeType.BYTES) < 0);

        assertTrue(NativeType.UUID.compareTo(NativeType.STRING) < 0);
        assertTrue(NativeType.UUID.compareTo(NativeType.BYTES) < 0);
    }

    /**
     *
     */
    @Test
    public void testCompareFixlenBySize() {
        assertTrue(NativeType.SHORT.compareTo(NativeType.INTEGER) < 0);
        assertTrue(NativeType.INTEGER.compareTo(NativeType.LONG) < 0);
        assertTrue(NativeType.LONG.compareTo(NativeType.UUID) < 0);
    }

    /**
     *
     */
    @Test
    public void testCompareFixlenByDesc() {
        assertTrue(NativeType.FLOAT.compareTo(NativeType.INTEGER) < 0);
    }

    /**
     *
     */
    @Test
    public void testCompareVarlenByDesc() {
        assertTrue(NativeType.BYTES.compareTo(NativeType.STRING) < 0);
    }
}
