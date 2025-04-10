/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.e2e.framework.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

/** SqlTestUtils. */
public class SqlTestUtils {
    private static final Random RANDOM = new Random();

    /** sqlCountRecordsTest. */
    public static void sqlCountRecordsTest(Connection conn, String tableName, int expectedRecords) throws SQLException {
        try (var stm = conn.createStatement()) {
            ResultSet rs = stm.executeQuery("SELECT COUNT(*) FROM " + tableName + ";");
            // Get the record
            assertTrue(rs.next());
            assertEquals(expectedRecords, rs.getLong(1));
        }
    }

    /** sqlRandomElementTest. */
    public static <V> void sqlRandomElementTest(
            Connection conn,
            String tableName,
            String keyColumnName,
            int numGeneratedExamples,
            IntFunction<Map.Entry<?, V>> exampleSupplier,
            BiConsumer<V, ResultSet> assertResultSet) throws SQLException {
        // Get a random element out of the DB
        int randomSeed = RANDOM.nextInt(numGeneratedExamples);
        var sample = exampleSupplier.apply(randomSeed);

        var expectedObj = sample.getValue();

        try (var stm = conn.prepareStatement(String.format("SELECT * FROM %s WHERE %s = ?;", tableName, keyColumnName))) {
            stm.setObject(1, sample.getKey());
            ResultSet rs = stm.executeQuery();

            // Get the record
            assertTrue(rs.next());
            // Assert the fields
            assertResultSet.accept(expectedObj, rs);
        }
    }
}
