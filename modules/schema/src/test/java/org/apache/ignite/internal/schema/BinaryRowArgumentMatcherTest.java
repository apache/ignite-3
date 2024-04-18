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

package org.apache.ignite.internal.schema;

import static org.apache.ignite.internal.schema.BinaryRowArgumentMatcher.equalToRow;
import static org.apache.ignite.internal.schema.SchemaTestUtils.binaryRow;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** For {@link BinaryRowArgumentMatcher} testing. */
public class BinaryRowArgumentMatcherTest extends BaseIgniteAbstractTest {
    private static final SchemaDescriptor SCHEMA_1 = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", INT32, false)},
            new Column[]{new Column("value", INT32, false)}
    );

    private static final SchemaDescriptor SCHEMA_2 = new SchemaDescriptor(
            2,
            new Column[]{new Column("key", INT32, false)},
            new Column[]{new Column("value", INT32, false)}
    );

    @Test
    void testMatches() {
        BinaryRow row0 = binaryRow(SCHEMA_1, 1, 2);
        BinaryRow row1 = binaryRow(SCHEMA_1, 1, 2);

        assertTrue(equalToRow(row0).matches(row1));
        assertTrue(equalToRow(row1).matches(row0));
    }

    @Test
    void testNotMatchesBySchemaVersion() {
        BinaryRow row0 = binaryRow(SCHEMA_1, 1, 2);
        BinaryRow row1 = binaryRow(SCHEMA_2, 1, 2);

        assertFalse(equalToRow(row0).matches(row1));
        assertFalse(equalToRow(row1).matches(row0));
    }

    @Test
    void testNotMatchesByTupleSlice() {
        BinaryRow row0 = binaryRow(SCHEMA_1, 1, 2);
        BinaryRow row1 = binaryRow(SCHEMA_1, 3, 4);

        assertFalse(equalToRow(row0).matches(row1));
        assertFalse(equalToRow(row1).matches(row0));
    }
}
