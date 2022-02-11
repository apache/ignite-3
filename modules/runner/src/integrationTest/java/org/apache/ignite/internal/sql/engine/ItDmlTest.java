package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsSubPlan;

import org.junit.jupiter.api.Test;

public class ItDmlTest extends AbstractBasicIntegrationTest {

    protected int nodes() {
        return 1;
    }

    @Test
    public void test0() {
        sql("CREATE TABLE test2 (k1 int, k2 int, a int, b varchar, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test2 VALUES (333, 333, 0, '', '100')");
        sql("INSERT INTO test2 VALUES (444, 444, 2, '', '200')");

        assertQuery("SELECT * FROM test2")
                .returns(333, 333, 0, "", "100")
                .returns(444, 444, 2, "", "200")
                .check();
    }

    /**
     * Test full MERGE command.
     */
    @Test
    public void testMerge() {
/*        sql("CREATE TABLE test1 (k1 int, k2 int, a int, b varchar, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test1 VALUES (111, 111, 0, 'a', '0')");
        sql("INSERT INTO test1 VALUES (222, 222, 1, 'b', '1')");

        sql("CREATE TABLE test2 (k1 int, k2 int, a int, b varchar, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test2 VALUES (333, 333, 0, '100', '')");
        sql("INSERT INTO test2 VALUES (444, 444, 2, '200', '')");

        String sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a " +
                "WHEN MATCHED THEN UPDATE SET b = src.b " +
                "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b) VALUES (src.k1, src.k2, src.a, src.b)";

        assertQuery(sql).matches(containsSubPlan("IgniteTableSpool")).check();

        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns(222, 222, 1, "b", null)
                .returns(333, 333, 0, "a", "")
                .returns(444, 444, 2, "200", "")
                .check();

*//*        sql("DROP TABLE test2");
        sql("CREATE TABLE test2 (k1 int, k2 int, a int, b varchar, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test2 VALUES (333, 333, 0, '100', '')");
        sql("INSERT INTO test2 VALUES (444, 444, 2, '200', '')");

        sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a " +
                "WHEN MATCHED THEN UPDATE SET b = src.b, k1 = src.k1 " +
                "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b) VALUES (src.k1, src.k2, src.a, src.b)";

        sql(sql);

        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns(222, 222, 1, "b", null)
                .returns(111, 333, 0, "a", "")
                .returns(444, 444, 2, "200", "")
                .check();*//*

        // ----

        sql("DROP TABLE test2");
        sql("CREATE TABLE test2 (k1 int, k2 int, a int, b varchar, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test2 VALUES (333, 333, 0, '100', '')");
        sql("INSERT INTO test2 VALUES (444, 444, 2, '200', '')");

        // the same but ON fld append to MATCHED
        sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a " +
                "WHEN MATCHED THEN UPDATE SET b = src.b, a = src.a " +
                "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b) VALUES (src.k1, src.k2, src.a, src.b)";

        sql(sql);

        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns(222, 222, 1, "b", null)
                .returns(333, 333, 0, "a", "")
                .returns(444, 444, 2, "200", "")
                .check();

        // ----

        sql("DROP TABLE test2");
        sql("CREATE TABLE test2 (k1 int, k2 int, a int, b varchar, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test2 VALUES (333, 333, 0, '100', '')");
        sql("INSERT INTO test2 VALUES (444, 444, 2, '200', '')");

        // all flds covered on NOT MATCHED
        sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a " +
                "WHEN MATCHED THEN UPDATE SET b = src.b, a = src.a " +
                "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b, c) VALUES (src.k1, src.k2, src.a, src.b, src.c)";

        sql(sql);

        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns(222, 222, 1, "b", "1")
                .returns(333, 333, 0, "a", "")
                .returns(444, 444, 2, "200", "")
                .check();

        // ----

        sql("DROP TABLE test1");
        sql("DROP TABLE test2");*/

        sql("CREATE TABLE test1 (k1 int, k2 int, a int, b int, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test1 VALUES (111, 111, 0, 100, '0')");
        sql("INSERT INTO test1 VALUES (222, 222, 1, 300, '1')");

        sql("CREATE TABLE test2 (k1 int, k2 int, a int, b int, c varchar, CONSTRAINT PK PRIMARY KEY (k1, k2))");
        sql("INSERT INTO test2 VALUES (333, 333, 0, 100, '')");
        sql("INSERT INTO test2 VALUES (444, 444, 2, 200, '')");

        // varlen in pk
/*        String sql = "MERGE INTO test2 dst USING test1 src ON dst.a = src.a " +
                "WHEN MATCHED THEN UPDATE SET b = 100 * src.b " +
                "WHEN NOT MATCHED THEN INSERT (k1, k2, a, b, c) VALUES (src.k1, src.k2, 10 * src.a, src.b, src.c)";

        sql(sql);*/

/*        assertQuery("SELECT * FROM test2 ORDER BY k1")
                .returns("222")
                .check();*/
    }
}
