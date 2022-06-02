package org.apache.ignite.cli.sql.table;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TableTest {

    @Test
    public void headerTest() {
        Table<String> table = new Table<>(List.of("foo", "bar"), List.of("1", "2"));
        Assertions.assertArrayEquals(table.header(), new Object[] {"foo", "bar"});
    }

    @Test
    public void contentTest() {
        Table<String> table = new Table<>(List.of("foo", "bar"), List.of("1", "2"));
        Assertions.assertArrayEquals(table.content(), new Object[][]{new Object[]{"1", "2"}});
    }

    @Test
    public void emptyTableTest() {
        Table<String> table = new Table<>(Collections.emptyList(), Collections.emptyList());
        Assertions.assertArrayEquals(table.header(), new String[] { "EMPTY" });
        Assertions.assertArrayEquals(table.content(), new Object[0][0]);
    }
}