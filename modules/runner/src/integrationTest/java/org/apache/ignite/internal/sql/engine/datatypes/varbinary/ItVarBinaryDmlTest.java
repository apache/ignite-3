package org.apache.ignite.internal.sql.engine.datatypes.varbinary;

import static org.apache.ignite.lang.IgniteStringFormatter.format;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.datatypes.DataTypeTestSpecs;
import org.apache.ignite.internal.sql.engine.datatypes.tests.BaseDmlDataTypeTest;
import org.apache.ignite.internal.sql.engine.datatypes.tests.DataTypeTestSpec;
import org.junit.jupiter.api.Test;

/**
 * Tests for expressions for {@link SqlTypeName#VARBINARY} type.
 */
public class ItVarBinaryDmlTest extends BaseDmlDataTypeTest<VarBinary> {

    /** {@code INSERT} into DEFAULT column */
    @Test
    public void testDefault() {
        runSql(format("CREATE TABLE t_def (id INT PRIMARY KEY, val VARBINARY DEFAULT $0_lit)"));
        runSql("INSERT INTO t_def (id) VALUES (0)");

        VarBinary value = values.get(0);
        checkQuery("SELECT val FROM t_def WHERE id=0").returns(value).check();
    }

    /** {@inheritDoc} */
    @Override
    protected DataTypeTestSpec<VarBinary> getTypeSpec() {
        return DataTypeTestSpecs.VARBINARY_TYPE;
    }
}
