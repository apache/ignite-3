package org.apache.ignite.internal.matcher;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.catalog.ColumnType;
import org.apache.ignite.catalog.definitions.ColumnDefinition;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

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
