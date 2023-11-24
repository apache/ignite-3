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

package org.apache.ignite.internal.table.distributed.replicator;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PRECISION;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_SCALE;
import static org.apache.ignite.internal.table.distributed.replicator.TypeConvertibility.typeChangeIsLossless;
import static org.apache.ignite.sql.ColumnType.DECIMAL;
import static org.apache.ignite.sql.ColumnType.INT16;
import static org.apache.ignite.sql.ColumnType.INT32;
import static org.apache.ignite.sql.ColumnType.NUMBER;
import static org.apache.ignite.sql.ColumnType.STRING;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.Test;

class TypeConvertibilityTest {
    @Test
    void losslessConversions() {
        // NUMBER -> INT16
        assertTrue(typeChangeIsLossless(number(4, 0), simple(INT16)));
        assertFalse(typeChangeIsLossless(number(5, 0), simple(INT16)));
        assertTrue(typeChangeIsLossless(number(3, -1), simple(INT16)));
        assertFalse(typeChangeIsLossless(number(4, -1), simple(INT16)));

        // NUMBER -> INT32
        assertTrue(typeChangeIsLossless(number(8, 0), simple(INT32)));
        assertFalse(typeChangeIsLossless(number(9, 0), simple(INT32)));
        assertTrue(typeChangeIsLossless(number(7, -1), simple(INT32)));
        assertFalse(typeChangeIsLossless(number(8, -1), simple(INT32)));

        // DECIMAL -> INT32
        assertTrue(typeChangeIsLossless(decimal(8, 0), simple(INT32)));
        assertFalse(typeChangeIsLossless(decimal(9, 0), simple(INT32)));
        assertTrue(typeChangeIsLossless(decimal(7, -1), simple(INT32)));
        assertFalse(typeChangeIsLossless(decimal(8, -1), simple(INT32)));

        assertFalse(typeChangeIsLossless(decimal(1, 1), simple(INT32)));

        // NUMBER -> DECIMAL
        assertTrue(typeChangeIsLossless(number(10, 0), decimal(10, 0)));
        assertFalse(typeChangeIsLossless(number(10, 0), decimal(9, 0)));
        assertTrue(typeChangeIsLossless(number(10, 0), decimal(12, 2)));
        assertFalse(typeChangeIsLossless(number(10, 0), decimal(11, 2)));
        assertTrue(typeChangeIsLossless(number(10, -2), decimal(10, -2)));
        assertFalse(typeChangeIsLossless(number(10, -2), decimal(9, -2)));
        assertTrue(typeChangeIsLossless(number(10, -2), decimal(12, 0)));
        assertFalse(typeChangeIsLossless(number(10, -2), decimal(11, 0)));
        assertTrue(typeChangeIsLossless(number(10, -2), decimal(14, 2)));
        assertFalse(typeChangeIsLossless(number(10, -2), decimal(13, 2)));

        assertFalse(typeChangeIsLossless(number(10, -2), decimal(9, -3)));
        assertTrue(typeChangeIsLossless(number(10, -2), decimal(11, -1)));
        assertTrue(typeChangeIsLossless(number(10, -2), decimal(13, 1)));

        // DECIMAL -> NUMBER
        assertTrue(typeChangeIsLossless(decimal(10, 0), number(10, 0)));
        assertFalse(typeChangeIsLossless(decimal(10, 0), number(9, 0)));
        assertFalse(typeChangeIsLossless(decimal(12, 2), number(10, 0)));
        assertTrue(typeChangeIsLossless(decimal(10, -2), number(10, -2)));
        assertFalse(typeChangeIsLossless(decimal(10, -2), number(9, -2)));
        assertTrue(typeChangeIsLossless(decimal(10, -2), number(12, 0)));
        assertFalse(typeChangeIsLossless(decimal(10, -2), number(11, 0)));
        assertFalse(typeChangeIsLossless(decimal(14, 2), number(10, -2)));

        assertFalse(typeChangeIsLossless(decimal(10, -2), number(9, -3)));
        assertTrue(typeChangeIsLossless(decimal(10, -2), number(11, -1)));
        assertFalse(typeChangeIsLossless(decimal(13, 1), number(10, -2)));

        // INT -> STRING
        assertTrue(typeChangeIsLossless(simple(INT32), string(10)));
        assertFalse(typeChangeIsLossless(simple(INT32), string(9)));

        // NUMBER -> STRING
        assertTrue(typeChangeIsLossless(number(10, 0), string(11)));
        assertFalse(typeChangeIsLossless(number(10, 0), string(10)));
        assertTrue(typeChangeIsLossless(number(10, -1), string(12)));
        assertFalse(typeChangeIsLossless(number(10, -1), string(11)));

        // DECIMAL -> STRING
        assertTrue(typeChangeIsLossless(decimal(10, 0), string(11)));
        assertFalse(typeChangeIsLossless(decimal(10, 0), string(10)));
        assertTrue(typeChangeIsLossless(decimal(10, -1), string(12)));
        assertFalse(typeChangeIsLossless(decimal(10, -1), string(11)));
        assertTrue(typeChangeIsLossless(decimal(10, 1), string(12)));
        assertFalse(typeChangeIsLossless(decimal(10, 1), string(11)));
    }

    private CatalogTableColumnDescriptor simple(ColumnType type) {
        return column(type, DEFAULT_PRECISION, DEFAULT_SCALE, DEFAULT_LENGTH);
    }

    private CatalogTableColumnDescriptor decimal(int precision, int scale) {
        return column(DECIMAL, precision, scale, DEFAULT_LENGTH);
    }

    private CatalogTableColumnDescriptor number(int precision, int scale) {
        return column(NUMBER, precision, scale, DEFAULT_LENGTH);
    }

    private CatalogTableColumnDescriptor string(int length) {
        return column(STRING, DEFAULT_PRECISION, DEFAULT_SCALE, length);
    }

    private CatalogTableColumnDescriptor column(ColumnType type, int precision, int scale, int length) {
        return new CatalogTableColumnDescriptor("name", type, true, precision, scale, length, null);
    }
}
