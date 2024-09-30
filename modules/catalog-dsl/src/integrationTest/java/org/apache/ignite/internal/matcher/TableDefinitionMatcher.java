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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;

import java.util.List;
import org.apache.ignite.catalog.ColumnSorted;
import org.apache.ignite.catalog.IndexType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.apache.ignite.catalog.definitions.IndexDefinition;
import org.apache.ignite.catalog.definitions.TableDefinition;
import org.apache.ignite.internal.testframework.matchers.AnythingMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher implementation for {@link TableDefinition}.
 */
public class TableDefinitionMatcher extends TypeSafeMatcher<TableDefinition> {
    private Matcher<String> tableNameMatcher = AnythingMatcher.anything();

    private Matcher<String> schemaNameMatcher = AnythingMatcher.anything();

    private Matcher<Boolean> ifNotExistsMatcher = AnythingMatcher.anything();

    private Matcher<List<ColumnDefinition>> columnsMatcher = AnythingMatcher.anything();

    private Matcher<IndexType> pkTypeMatcher = AnythingMatcher.anything();

    private Matcher<List<ColumnSorted>> pkColumnsMatcher = AnythingMatcher.anything();

    private Matcher<List<String>> colocationColumnsMatcher = AnythingMatcher.anything();

    private Matcher<String> zoneNameMatcher = AnythingMatcher.anything();

    private Matcher<Class<?>> keyClassMatcher = AnythingMatcher.anything();

    private Matcher<Class<?>> valueClassMatcher = AnythingMatcher.anything();

    private Matcher<List<IndexDefinition>> indexesMatcher = AnythingMatcher.anything();

    public TableDefinitionMatcher withTableNameMatcher(Matcher<String> tableNameMatcher) {
        this.tableNameMatcher = tableNameMatcher;
        return this;
    }

    public TableDefinitionMatcher withTableName(String tableName) {
        return withTableNameMatcher(equalToIgnoringCase(tableName));
    }

    public TableDefinitionMatcher withSchemaNameMatcher(Matcher<String> schemaNameMatcher) {
        this.schemaNameMatcher = schemaNameMatcher;
        return this;
    }

    public TableDefinitionMatcher withSchemaName(String schemaName) {
        return withSchemaNameMatcher(equalToIgnoringCase(schemaName));
    }

    public TableDefinitionMatcher withIfNotExistsMatcher(Matcher<Boolean> ifNotExists) {
        this.ifNotExistsMatcher = ifNotExists;
        return this;
    }

    public TableDefinitionMatcher withIfNotExists(boolean ifNotExists) {
        return withIfNotExistsMatcher(equalTo(ifNotExists));
    }

    public TableDefinitionMatcher withColumnsMatcher(Matcher<List<ColumnDefinition>> columns) {
        this.columnsMatcher = columns;
        return this;
    }

    public TableDefinitionMatcher withColumns(List<ColumnDefinition> columns) {
        return withColumnsMatcher(new ColumnDefinitionsMatcher(columns));
    }

    public TableDefinitionMatcher withPkTypeMatcher(Matcher<IndexType> pkType) {
        this.pkTypeMatcher = pkType;
        return this;
    }

    public TableDefinitionMatcher withPkType(IndexType pkType) {
        return withPkTypeMatcher(equalTo(pkType));
    }

    public TableDefinitionMatcher withPkColumnsMatcher(Matcher<List<ColumnSorted>> pkColumns) {
        this.pkColumnsMatcher = pkColumns;
        return this;
    }

    public TableDefinitionMatcher withPkColumns(List<ColumnSorted> pkColumns) {
        return withPkColumnsMatcher(equalTo(pkColumns));
    }

    public TableDefinitionMatcher withColocationColumnsMatcher(Matcher<List<String>> colocationColumns) {
        this.colocationColumnsMatcher = colocationColumns;
        return this;
    }

    public TableDefinitionMatcher withColocationColumns(List<String> colocationColumns) {
        return withColocationColumnsMatcher(equalTo(colocationColumns));
    }

    public TableDefinitionMatcher withZoneNameMatcher(Matcher<String> zoneName) {
        this.zoneNameMatcher = zoneName;
        return this;
    }

    public TableDefinitionMatcher withZoneName(String zoneName) {
        return withZoneNameMatcher(equalTo(zoneName));
    }

    public TableDefinitionMatcher withKeyClassMatcher(Matcher<Class<?>> keyClass) {
        this.keyClassMatcher = keyClass;
        return this;
    }

    public TableDefinitionMatcher withKeyClass(Class<?> keyClass) {
        return withKeyClassMatcher(equalTo(keyClass));
    }

    public TableDefinitionMatcher withValueClassMatcher(Matcher<Class<?>> valueClass) {
        this.valueClassMatcher = valueClass;
        return this;
    }

    public TableDefinitionMatcher withValueClass(Class<?> valueClass) {
        return withValueClassMatcher(equalTo(valueClass));
    }

    public TableDefinitionMatcher withIndexesMatcher(Matcher<List<IndexDefinition>> indexes) {
        this.indexesMatcher = indexes;
        return this;
    }

    public TableDefinitionMatcher withIndexes(List<IndexDefinition> indexes) {
        return withIndexesMatcher(equalTo(indexes));
    }

    public static TableDefinitionMatcher isTableDefinition() {
        return new TableDefinitionMatcher();
    }

    @Override
    protected boolean matchesSafely(TableDefinition tableDefinition) {
        return tableNameMatcher.matches(tableDefinition.tableName())
                && schemaNameMatcher.matches(tableDefinition.schemaName())
                && ifNotExistsMatcher.matches(tableDefinition.ifNotExists())
                && columnsMatcher.matches(tableDefinition.columns())
                && pkTypeMatcher.matches(tableDefinition.primaryKeyType())
                && pkColumnsMatcher.matches(tableDefinition.primaryKeyColumns())
                && colocationColumnsMatcher.matches(tableDefinition.colocationColumns())
                && zoneNameMatcher.matches(tableDefinition.zoneName())
                && keyClassMatcher.matches(tableDefinition.keyClass())
                && valueClassMatcher.matches(tableDefinition.valueClass())
                && indexesMatcher.matches(tableDefinition.indexes());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Table definition with ")
                .appendText("table name ").appendDescriptionOf(tableNameMatcher)
                .appendText(System.lineSeparator())
                .appendText("schema name ").appendDescriptionOf(schemaNameMatcher)
                .appendText(System.lineSeparator())
                .appendText("if not exists ").appendDescriptionOf(ifNotExistsMatcher)
                .appendText(System.lineSeparator())
                .appendText("columns ").appendDescriptionOf(columnsMatcher)
                .appendText(System.lineSeparator())
                .appendText("pk type ").appendDescriptionOf(pkTypeMatcher)
                .appendText(System.lineSeparator())
                .appendText("pk columns ").appendDescriptionOf(pkColumnsMatcher)
                .appendText(System.lineSeparator())
                .appendText("colocation columns ").appendDescriptionOf(columnsMatcher)
                .appendText(System.lineSeparator())
                .appendText("zone name ").appendDescriptionOf(zoneNameMatcher)
                .appendText(System.lineSeparator())
                .appendText("key class ").appendDescriptionOf(keyClassMatcher)
                .appendText(System.lineSeparator())
                .appendText("value class ").appendDescriptionOf(valueClassMatcher)
                .appendText(System.lineSeparator())
                .appendText("indexes ").appendDescriptionOf(indexesMatcher);
    }
}
