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

package org.apache.ignite.internal.storage;

import static org.apache.ignite.internal.storage.BinaryRowAndRowIdMatcher.equalToBinaryRowAndRowId;
import static org.apache.ignite.internal.storage.RowId.highestRowId;
import static org.apache.ignite.internal.storage.RowId.lowestRowId;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.junit.jupiter.api.Test;

/** For {@link BinaryRowAndRowIdMatcher} testing. */
public class BinaryRowAndRowIdMatcherTest {
    private static final int PARTITION_ID = 0;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", INT32, false)},
            new Column[]{new Column("value", INT32, false)}
    );

    @Test
    void testNotMatchByRowId() {
        var binaryRowAndRowId0 = new BinaryRowAndRowId(null, lowestRowId(PARTITION_ID));
        var binaryRowAndRowId1 = new BinaryRowAndRowId(null, highestRowId(PARTITION_ID));

        assertThat(binaryRowAndRowId0, not(equalToBinaryRowAndRowId(binaryRowAndRowId1)));
        assertThat(binaryRowAndRowId1, not(equalToBinaryRowAndRowId(binaryRowAndRowId0)));
    }

    @Test
    void testNotMatchByBinaryRow() {
        var binaryRowAndRowId0 = new BinaryRowAndRowId(binaryRow(0, 1), lowestRowId(PARTITION_ID));
        var binaryRowAndRowId1 = new BinaryRowAndRowId(binaryRow(5, 6), lowestRowId(PARTITION_ID));
        var binaryRowAndRowId2 = new BinaryRowAndRowId(null, lowestRowId(PARTITION_ID));

        assertThat(binaryRowAndRowId0, not(equalToBinaryRowAndRowId(binaryRowAndRowId1)));
        assertThat(binaryRowAndRowId1, not(equalToBinaryRowAndRowId(binaryRowAndRowId0)));

        assertThat(binaryRowAndRowId1, not(equalToBinaryRowAndRowId(binaryRowAndRowId2)));
        assertThat(binaryRowAndRowId2, not(equalToBinaryRowAndRowId(binaryRowAndRowId1)));
    }

    @Test
    void testMatch() {
        var binaryRowAndRowId0 = new BinaryRowAndRowId(binaryRow(0, 1), lowestRowId(PARTITION_ID));
        var binaryRowAndRowId1 = new BinaryRowAndRowId(binaryRow(0, 1), lowestRowId(PARTITION_ID));

        assertThat(binaryRowAndRowId0, equalToBinaryRowAndRowId(binaryRowAndRowId1));
        assertThat(binaryRowAndRowId1, equalToBinaryRowAndRowId(binaryRowAndRowId0));
    }

    private static BinaryRow binaryRow(int key, int val) {
        return SchemaTestUtils.binaryRow(SCHEMA, key, val);
    }
}
