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

package org.apache.ignite.internal.sql.engine.exec.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/** Tests for {@link RowSchema}. */
public class RowSchemaTest {

    /** Schema equality. */
    @Test
    public void testSchemaEquality() {
        RowSchema s1 = RowSchema.builder()
                .addField(new BaseTypeSpec(NativeTypes.INT32))
                .build();
        assertEquals(s1, s1);

        List<BaseTypeSpec> fields = List.of(new BaseTypeSpec(NativeTypes.INT32));
        assertEquals(fields, s1.fields());

        RowSchema s2 = RowSchema.builder()
                .addField(new BaseTypeSpec(NativeTypes.INT32))
                .build();

        assertEquals(s1, s2);
    }

    /** Schema inequality. */
    @Test
    public void testSchemaInequality() {
        {
            RowSchema s1 = RowSchema.builder()
                    .addField(new BaseTypeSpec(NativeTypes.INT32))
                    .build();

            RowSchema s2 = RowSchema.builder()
                    .addField(new BaseTypeSpec(NativeTypes.STRING))
                    .build();

            assertNotEquals(s1, s2);
        }

        {
            RowSchema s1 = RowSchema.builder()
                    .addField(new BaseTypeSpec(NativeTypes.INT32))
                    .build();

            RowSchema s2 = RowSchema.builder()
                    .addField(new BaseTypeSpec(NativeTypes.INT32))
                    .addField(new BaseTypeSpec(NativeTypes.INT32))
                    .build();

            assertNotEquals(s1, s2);
        }
    }
}
