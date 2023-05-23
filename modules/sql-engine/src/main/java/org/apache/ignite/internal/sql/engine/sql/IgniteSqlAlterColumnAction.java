package org.apache.ignite.internal.sql.engine.sql;

import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

public abstract class IgniteSqlAlterColumnAction extends SqlDdl {
    /** Alter operator. */
    private static final SqlOperator OPERATOR =
            new SqlSpecialOperator("ALTER COLUMN", SqlKind.ALTER_TABLE);

    /** Default constructor. */
    protected IgniteSqlAlterColumnAction(SqlParserPos pos) {
        super(OPERATOR, pos);
    }
}
