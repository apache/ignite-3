package org.apache.ignite.internal.sql.engine.sql.fun;

import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAbstractGroupFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

class SqlGroupingFunction extends SqlAbstractGroupFunction {
    SqlGroupingFunction(String name) {
        super(name, SqlKind.GROUPING, ReturnTypes.BIGINT, null,
                OperandTypes.ONE_OR_MORE, SqlFunctionCategory.SYSTEM);
    }
}