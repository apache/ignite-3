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

package org.apache.ignite.internal.configuration.validation;

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.internal.configuration.SystemPropertyView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** For {@link LongNumberSystemPropertyValueValidator} testing. */
public class LongNumberSystemPropertyValueValidatorTest extends BaseIgniteAbstractTest {
    private static final String FOO_KEY = "foo";

    private static final String BAR_KEY = "bar";

    private final LongNumberSystemPropertyValueValidator validator = new LongNumberSystemPropertyValueValidator(FOO_KEY, BAR_KEY);

    @Test
    void testWrongValue() {
        validate(
                validator,
                mock(NamedConfigValue.class),
                mockValidationContext(null, mockNamedListView(List.of(FOO_KEY, BAR_KEY), List.of("not long value", "not long value"))),
                String.format(
                        "'%s' system property value must be a long number '%s'",
                        null, FOO_KEY
                ),
                String.format(
                        "'%s' system property value must be a long number '%s'",
                        null, BAR_KEY
                )
        );
    }

    @Test
    void testCorrectValue() {
        validate(
                validator,
                mock(NamedConfigValue.class),
                mockValidationContext(null, mockNamedListView(List.of(FOO_KEY, BAR_KEY), List.of("10", "-20")))
        );
    }

    @Test
    void testUnknownKeys() {
        validate(
                validator,
                mock(NamedConfigValue.class),
                mockValidationContext(null, mockNamedListView(List.of("fakeOne", "fakeTwo"), List.of("10", "not long value")))
        );
    }

    private static NamedListView<SystemPropertyView> mockNamedListView(List<String> names, List<String> values) {
        assertEquals(names.size(), values.size());

        Map<String, SystemPropertyView> systemPropertyViewByName = IntStream.range(0, names.size())
                .mapToObj(i -> {
                    SystemPropertyView systemPropertyView = mock(SystemPropertyView.class);

                    String name = names.get(i);
                    String value = values.get(i);

                    when(systemPropertyView.name()).thenReturn(name);
                    when(systemPropertyView.propertyValue()).thenReturn(value);

                    return systemPropertyView;
                })
                .collect(Collectors.toMap(SystemPropertyView::name, Function.identity()));

        NamedListView<SystemPropertyView> namedListView = mock(NamedListView.class);

        when(namedListView.namedListKeys()).thenReturn(names);

        when(namedListView.get(any(String.class))).then(invocation -> systemPropertyViewByName.get(invocation.getArgument(0)));

        return namedListView;
    }
}
