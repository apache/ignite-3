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

package org.apache.ignite.internal.sql.engine.util;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnMetadata.ColumnOrigin;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.function.Executable;

/**
 * Column metadata checker.
 */
public class MetadataMatcher implements ColumnMatcher {
    /** Marker object. */
    private static final Object NO_CHECK = new Object() {
        @Override
        public String toString() {
            return "<non_checkable>";
        }
    };

    /** Name of the result's column. */
    private Object name = NO_CHECK;

    /** Type of the result's column. */
    private Object type = NO_CHECK;

    /** Column precision. */
    private Object precision = NO_CHECK;

    /** Column scale. */
    private Object scale = NO_CHECK;

    /** Nullable flag of the result's column. */
    private Object nullable = NO_CHECK;

    /** Origin of the result's column. */
    private Object origin = NO_CHECK;

    /**
     * Sets column name.
     *
     * @return This.
     */
    public MetadataMatcher name(String name) {
        this.name = name;

        return this;
    }

    /**
     * Sets column SQL type.
     *
     * @return This.
     */
    public MetadataMatcher type(ColumnType type) {
        this.type = type;

        return this;
    }

    /**
     * Sets column precision.
     *
     * @return This.
     */
    public MetadataMatcher precision(int precision) {
        this.precision = precision;

        return this;
    }

    /**
     * Sets column scale.
     *
     * @return This.
     */
    public MetadataMatcher scale(int scale) {
        this.scale = scale;

        return this;
    }

    /**
     * Sets column nullability flag.
     *
     * @return This.
     */
    public MetadataMatcher nullable(boolean nullable) {
        this.nullable = nullable;

        return this;
    }

    /**
     * Sets column origins.
     *
     * @return This.
     */
    public MetadataMatcher origin(ColumnOrigin origin) {
        this.origin = origin;

        return this;
    }

    /**
     * Checks metadata.
     *
     * @param actualMeta Metadata to check.
     */
    @Override
    public void check(ColumnMetadata actualMeta) {
        List<Executable> matchers = new ArrayList<>();

        if (name != NO_CHECK) {
            matchers.add(() -> assertEquals(name, actualMeta.name(), "name"));
        }

        if (type != NO_CHECK) {
            ColumnType type0 = (ColumnType) type;
            matchers.add(() -> assertSame(type0, actualMeta.type(), "type"));
            matchers.add(() -> assertSame(type0.javaClass(), actualMeta.valueClass(), "value class"));
        }

        if (precision != NO_CHECK) {
            matchers.add(() -> assertEquals(precision, actualMeta.precision(), "precision"));
        }

        if (scale != NO_CHECK) {
            matchers.add(() -> assertEquals(scale, actualMeta.scale(), "scale"));
        }

        if (nullable != NO_CHECK) {
            matchers.add(() -> assertEquals(nullable, actualMeta.nullable(), "nullable"));
        }

        if (origin != NO_CHECK) {
            if (origin == null) {
                matchers.add(() -> assertNull(actualMeta.origin(), "origin"));
            } else {
                ColumnOrigin expected = (ColumnOrigin) origin;
                ColumnOrigin actual = actualMeta.origin();

                matchers.add(() -> {
                    assertNotNull(actual, "origin");
                    assertEquals(expected.schemaName(), actual.schemaName(), " origin schema");
                    assertEquals(expected.tableName(), actual.tableName(), " origin table");
                    assertEquals(expected.columnName(), actual.columnName(), " origin column");
                });
            }
        }

        assertAll("Mismatch:\n expected = " + this + ",\n actual = " + actualMeta, matchers);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(MetadataMatcher.class, this);
    }
}
