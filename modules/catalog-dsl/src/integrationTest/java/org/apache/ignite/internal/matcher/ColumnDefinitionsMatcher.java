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

package org.apache.ignite.internal.matcher;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher implementation of {@link ColumnDefinition}.
 */
public class ColumnDefinitionsMatcher extends TypeSafeMatcher<List<ColumnDefinition>> {

    private final Map<String, ColumnDefinition> columns;

    public ColumnDefinitionsMatcher(List<ColumnDefinition> columns) {
        this.columns = columns.stream().collect(Collectors.toMap(ColumnDefinition::name, Function.identity()));
    }

    @Override
    protected boolean matchesSafely(List<ColumnDefinition> item) {
        for (ColumnDefinition actualCol : item) {
            ColumnDefinition expectedCol = columns.get(actualCol.name());
            if (expectedCol == null) {
                return false;
            }

            ColumnType<?> actualType = actualCol.type();
            ColumnType<?> expectedType = expectedCol.type();
            if (expectedType != null && actualType != null && expectedType.type() != actualType.type()) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void describeTo(Description description) {

    }
}
