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

package org.apache.ignite.internal.sql.engine.property;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.internal.sql.engine.property.SqlProperties.Builder;
import org.apache.ignite.internal.util.Pair;
import org.junit.jupiter.api.Test;

/**
 * Test class to verify {@link SqlPropertiesHelper} class.
 */
class SqlPropertiesHelperTest {
    private static final String NON_STATIC_PROP_NAME = "non_static_prop";

    @SuppressWarnings("WeakerAccess")
    public static class TestProps {
        public static final Property<Long> LONG_PROP = new Property<>("long_prop", Long.class);
        public static final Property<Byte> BYTE_PROP = new Property<>("byte_prop", Byte.class);
        public static final Property<String> STRING_PROP = new Property<>("string_prop", String.class);
        private static final Property<String> PRIVATE_PROP = new Property<>("private_prop", String.class);
        static final Property<String> PROTECTED_PROP = new Property<>("protected_prop", String.class);

        @SuppressWarnings("unused")
        public final Property<String> nonStaticProp = new Property<>(NON_STATIC_PROP_NAME, String.class);
    }

    @Test
    public void buildPropMapFromClass() {
        var propMap = SqlPropertiesHelper.createPropsByNameMap(TestProps.class);

        assertFalse(propMap.isEmpty());

        {
            var knownPublicProps = List.of(
                    TestProps.LONG_PROP,
                    TestProps.BYTE_PROP,
                    TestProps.STRING_PROP
            );

            for (var expProp : knownPublicProps) {
                var actProp = propMap.get(expProp.name);

                assertEquals(expProp, actProp);
                assertSame(expProp.cls, actProp.cls);
            }
        }

        {
            var omittedProps = List.of(
                    TestProps.PRIVATE_PROP,
                    TestProps.PROTECTED_PROP
            );

            for (var prop : omittedProps) {
                assertNull(propMap.get(prop.name));
            }
        }

        {
            assertNull(propMap.get(NON_STATIC_PROP_NAME));
        }
    }

    @Test
    public void mergeEmptyHolders() {
        SqlProperties first = SqlPropertiesHelper.emptyProperties();
        SqlProperties second = SqlPropertiesHelper.emptyProperties();

        SqlProperties result = SqlPropertiesHelper.merge(first, second);

        assertThat(result, notNullValue());
        assertThat(result.iterator().hasNext(), is(false));
    }

    @Test
    public void mergeEmptyAndNonEmptyHolder() {
        SqlProperties empty = SqlPropertiesHelper.emptyProperties();
        SqlProperties nonEmpty = SqlPropertiesHelper.newBuilder()
                .set(TestProps.LONG_PROP, 42L)
                .build();

        List<Pair<SqlProperties, SqlProperties>> pairs = List.of(new Pair<>(empty, nonEmpty), new Pair<>(nonEmpty, empty));

        for (Pair<SqlProperties, SqlProperties> pair : pairs) {
            SqlProperties result = SqlPropertiesHelper.merge(pair.getFirst(), pair.getSecond());

            assertThat(result, notNullValue());

            Iterator<Entry<Property<?>, Object>> it = result.iterator();

            assertThat(it.hasNext(), is(true));

            Map.Entry<Property<?>, Object> entry = it.next();

            assertThat(entry.getKey(), is(TestProps.LONG_PROP));
            assertThat(entry.getValue(), is(42L));

            assertThat(it.hasNext(), is(false));
        }
    }

    @Test
    public void mergeNonEmptyWithConflict() {
        SqlProperties first = SqlPropertiesHelper.newBuilder()
                .set(TestProps.LONG_PROP, 1L)
                .set(TestProps.BYTE_PROP, (byte) 1)
                .build();

        SqlProperties second = SqlPropertiesHelper.newBuilder()
                .set(TestProps.LONG_PROP, 2L)
                .set(TestProps.STRING_PROP, "2")
                .build();

        SqlProperties result = SqlPropertiesHelper.merge(first, second);

        assertThat(result.get(TestProps.BYTE_PROP), is((byte) 1));
        assertThat(result.get(TestProps.LONG_PROP), is(1L));
        assertThat(result.get(TestProps.STRING_PROP), is("2"));

        result = SqlPropertiesHelper.merge(second, first);

        assertThat(result.get(TestProps.BYTE_PROP), is((byte) 1));
        assertThat(result.get(TestProps.LONG_PROP), is(2L));
        assertThat(result.get(TestProps.STRING_PROP), is("2"));
    }

    @Test
    public void chainEmptyHolders() {
        SqlProperties first = SqlPropertiesHelper.emptyProperties();
        SqlProperties second = SqlPropertiesHelper.emptyProperties();

        SqlProperties result = SqlPropertiesHelper.chain(first, second);

        assertThat(result, notNullValue());
        assertThat(result.iterator().hasNext(), is(false));
    }

    @Test
    public void chainEmptyAndNonEmptyHolder() {
        SqlProperties empty = SqlPropertiesHelper.emptyProperties();
        SqlProperties nonEmpty = SqlPropertiesHelper.newBuilder()
                .set(TestProps.LONG_PROP, 42L)
                .build();

        List<Pair<SqlProperties, SqlProperties>> pairs = List.of(new Pair<>(empty, nonEmpty), new Pair<>(nonEmpty, empty));

        for (Pair<SqlProperties, SqlProperties> pair : pairs) {
            SqlProperties result = SqlPropertiesHelper.chain(pair.getFirst(), pair.getSecond());

            assertThat(result, notNullValue());

            Iterator<Entry<Property<?>, Object>> it = result.iterator();

            assertThat(it.hasNext(), is(true));

            Map.Entry<Property<?>, Object> entry = it.next();

            assertThat(entry.getKey(), is(TestProps.LONG_PROP));
            assertThat(entry.getValue(), is(42L));

            assertThat(it.hasNext(), is(false));
        }
    }

    @Test
    public void chainNonEmptyWithConflict() {
        SqlProperties first = SqlPropertiesHelper.newBuilder()
                .set(TestProps.LONG_PROP, 1L)
                .set(TestProps.BYTE_PROP, (byte) 1)
                .build();

        SqlProperties second = SqlPropertiesHelper.newBuilder()
                .set(TestProps.LONG_PROP, 2L)
                .set(TestProps.STRING_PROP, "2")
                .build();

        SqlProperties result = SqlPropertiesHelper.chain(first, second);

        assertThat(result.get(TestProps.BYTE_PROP), is((byte) 1));
        assertThat(result.get(TestProps.LONG_PROP), is(1L));
        assertThat(result.get(TestProps.STRING_PROP), is("2"));

        result = SqlPropertiesHelper.merge(second, first);

        assertThat(result.get(TestProps.BYTE_PROP), is((byte) 1));
        assertThat(result.get(TestProps.LONG_PROP), is(2L));
        assertThat(result.get(TestProps.STRING_PROP), is("2"));
    }

    @Test
    public void createBuilderFromHolder() {
        SqlProperties base = SqlPropertiesHelper.newBuilder()
                .set(TestProps.BYTE_PROP, (byte) 1)
                .set(TestProps.LONG_PROP, 1L)
                .build();

        {
            SqlProperties newHolder = SqlPropertiesHelper.builderFromProperties(base)
                    .set(TestProps.STRING_PROP, "1")
                    .build();

            assertThat(newHolder.get(TestProps.BYTE_PROP), is((byte) 1));
            assertThat(newHolder.get(TestProps.LONG_PROP), is(1L));
            assertThat(newHolder.get(TestProps.STRING_PROP), is("1"));
            assertThat(newHolder, iterableWithSize(3));
        }

        {
            SqlProperties newHolder = SqlPropertiesHelper.builderFromProperties(base)
                    .set(TestProps.LONG_PROP, 2L)
                    .build();

            assertThat(newHolder.get(TestProps.BYTE_PROP), is((byte) 1));
            assertThat(newHolder.get(TestProps.LONG_PROP), is(2L));
            assertThat(newHolder, iterableWithSize(2));
        }
    }

    @Test
    public void valueIsValidatedInBuilder() {
        class PropertySetter {
            private Builder setProp(Builder builder, Property<?> property, Object value) {
                return builder.set((Property<? super Object>) property, value);
            }
        }

        PropertySetter setter = new PropertySetter();

        assertThrows(
                IllegalArgumentException.class,
                () -> setter.setProp(SqlPropertiesHelper.newBuilder(), TestProps.LONG_PROP, "42")
        );
    }
}
