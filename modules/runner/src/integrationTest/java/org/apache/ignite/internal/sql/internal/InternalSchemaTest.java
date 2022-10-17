package org.apache.ignite.internal.sql.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.schema.ExtendedTableView;
import org.apache.ignite.internal.sql.engine.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.junit.jupiter.api.Test;

/** Tests for internal manipulations with schema. */
public class InternalSchemaTest extends AbstractBasicIntegrationTest {
    /**
     * Checks that schema version is updated even if column names are intersected.
     */
    @Test
    public void checkSchemaUpdatedWithEqAlterColumn() {
        IgniteSql sql = igniteSql();
        Session ses = sql.createSession();

        checkDdl(true, ses, "CREATE TABLE TEST(ID INT PRIMARY KEY, VAL0 INT)");

        Ignite node = CLUSTER_NODES.get(0);

        ConfigurationManager cfgMgr = IgniteTestUtils.getFieldValue(node, "clusterCfgMgr");

        final TablesConfiguration tablesConfiguration = cfgMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY);

        int schIdBefore = ((ExtendedTableView) tablesConfiguration.tables().get("TEST").value()).schemaId();

        checkDdl(false, ses, "ALTER TABLE TEST ADD COLUMN IF NOT EXISTS (VAL0 INT, VAL1 INT)");

        int schIdAfter = ((ExtendedTableView) tablesConfiguration.tables().get("TEST").value()).schemaId();

        assertEquals(schIdBefore + 1, schIdAfter);
    }

    /** Test correct mapping schema after drop columns. */
    @Test
    public void testDropColumns() {
        IgniteSql sql = igniteSql();
        Session ses = sql.createSession();

        checkDdl(true, ses, "CREATE TABLE my (c1 INT PRIMARY KEY, c2 INT, c3 VARCHAR)");

        ses.execute(
                null,
                "INSERT INTO my VALUES (1, 1, '1')"
        );

        ResultSet res = ses.execute(
                null,
                "SELECT c1, c3 FROM my"
        );

        assertTrue(res.hasNext());

        checkDdl(true, ses, "ALTER TABLE my DROP COLUMN c2");

        res = ses.execute(
                null,
                "SELECT c1, c3 FROM my"
        );

        assertNotNull(res.next());

        checkDdl(true, ses, "ALTER TABLE my ADD COLUMN (c2 INT, c4 VARCHAR)");

        res = ses.execute(
                null,
                "SELECT c1, c3 FROM my"
        );

        assertNotNull(res.next());
    }

    private static void checkDdl(boolean expectedApplied, Session ses, String sql) {
        ResultSet res = ses.execute(
                null,
                sql
        );

        assertEquals(expectedApplied, res.wasApplied());
        assertFalse(res.hasRowSet());
        assertEquals(-1, res.affectedRows());

        res.close();
    }

    /**
     * Gets the SQL API.
     *
     * @return SQL API.
     */
    protected IgniteSql igniteSql() {
        return CLUSTER_NODES.get(0).sql();
    }
}
