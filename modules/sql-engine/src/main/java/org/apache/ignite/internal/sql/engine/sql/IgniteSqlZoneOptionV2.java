package org.apache.ignite.internal.sql.engine.sql;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

public class IgniteSqlZoneOptionV2 extends IgniteSqlZoneOption {
    /** ZONE option operator. */
    protected static class Operator extends IgniteSqlSpecialOperator {

        /** Constructor. */
        protected Operator() {
            super("ZoneOption V2", SqlKind.OTHER);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlZoneOption((SqlIdentifier) operands[0], operands[1], pos);
        }
    }

    private static final SqlOperator OPERATOR = new IgniteSqlZoneOption.Operator();

    /** Option key. */
    private final SqlIdentifier key;

    /** Option value. */
    private final SqlNode value;

    /** Creates {@link IgniteSqlZoneOption}. */
    public IgniteSqlZoneOptionV2(SqlIdentifier key, SqlNode value, SqlParserPos pos) {
        super(key, value, pos);

        assert key.isSimple() : key;

        this.key = key;
        this.value = value;
    }

/*    public IgniteSqlZoneOptionV2(IgniteSqlStorageProfile instance) {
        super(instance.key(), instance.value(), instance.getParserPosition());

        assert instance.key().isSimple() : instance.key();

        this.key = instance.key();
        this.value = instance.value();
    }*/

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return List.of(key, value);
    }

    /** {@inheritDoc} */
    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new IgniteSqlZoneOptionV2(key, value, pos);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        value.unparse(writer, leftPrec, rightPrec);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!(node instanceof IgniteSqlZoneOptionV2)) {
            return litmus.fail("{} != {}", this, node);
        }

        IgniteSqlZoneOptionV2 that = (IgniteSqlZoneOptionV2) node;
        if (key != that.key) {
            return litmus.fail("{} != {}", this, node);
        }

        return value.equalsDeep(that.value, litmus);
    }

    /**
     * Get option's key.
     */
    @Override
    public SqlIdentifier key() {
        return key;
    }

    /**
     * Get option's value.
     */
    @Override
    public SqlNode value() {
        return value;
    }
}
