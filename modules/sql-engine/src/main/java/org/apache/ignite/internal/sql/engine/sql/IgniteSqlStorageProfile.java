package org.apache.ignite.internal.sql.engine.sql;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

public class IgniteSqlStorageProfile extends SqlCall {
    /** ZONE profile operator. */
    protected static class Operator extends IgniteSqlSpecialOperator {

        /** Constructor. */
        protected Operator() {
            super("StorageProfiles", SqlKind.OTHER);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier, SqlParserPos pos, @Nullable SqlNode... operands) {
            return new IgniteSqlStorageProfile((SqlIdentifier) operands[0], (SqlNodeList) operands[1], pos);
        }
    }

    private static final SqlOperator OPERATOR = new Operator();

    /** Profile key. */
    private final SqlIdentifier key;

    /** Profile values. */
    private final SqlNodeList values;

    /** Creates {@link IgniteSqlStorageProfile}. */
    public IgniteSqlStorageProfile(SqlIdentifier key, SqlNodeList values, SqlParserPos pos) {
        super(pos);

        assert key.isSimple() : key;

        this.key = key;
        this.values = values;
        System.err.println();
    }

    /** {@inheritDoc} */
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return List.of(key, values);
    }

    /** {@inheritDoc} */
    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new IgniteSqlStorageProfile(key, values, pos);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);

        SqlWriter.Frame frame = writer.startList(FrameTypeEnum.SIMPLE, "[", "]");
        for (SqlNode c : values) {
            writer.sep(",");
            c.unparse(writer, 0, 0);
        }
        writer.endList(frame);

        //writer.list(SqlWriter.FrameTypeEnum.SIMPLE, SqlWriter.COMMA, values);

        System.err.println();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!(node instanceof IgniteSqlStorageProfile)) {
            return litmus.fail("{} != {}", this, node);
        }

        IgniteSqlStorageProfile that = (IgniteSqlStorageProfile) node;
        if (key != that.key) {
            return litmus.fail("{} != {}", this, node);
        }

        return values.equalsDeep(that.values, litmus);
    }

    /**
     * Get profile key.
     */
    public SqlIdentifier key() {
        return key;
    }

    /**
     * Get profile values.
     */
    public SqlNodeList value() {
        return values;
    }
}
