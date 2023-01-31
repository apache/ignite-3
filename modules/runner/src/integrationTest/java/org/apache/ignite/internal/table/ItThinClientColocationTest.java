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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.table.ItPublicApiColocationTest.generateValueByType;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests that client and server have matching colocation logic.
 */
public class ItThinClientColocationTest {
    @ParameterizedTest(name = "type=" + ARGUMENTS_PLACEHOLDER)
    @EnumSource(NativeTypeSpec.class)
    public void test(NativeTypeSpec type) {
        // TODO: Compare ClientTupleSerializer.getColocationHash and marshaller.marshal(t).colocationHash for various data types.
        for (int i = 0; i < 10; i++) {
            var val = generateValueByType(i, type);
            assertNotNull(val);
        }
    }
}
