package org.apache.ignite.internal.sql.engine.sql;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.jetbrains.annotations.Nullable;

public class IgniteSqlCreateZoneV2 extends SqlCreate {
    /** CREATE ZONE operator. */
    protected static class Operator extends IgniteDdlOperator {

        /** Constructor. */
        protected Operator(boolean existFlag) {
            super("CREATE ZONE V2", SqlKind.OTHER_DDL, existFlag);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                SqlParserPos pos, @Nullable SqlNode... operands) {

            return new IgniteSqlCreateZoneV2(pos, existFlag(), (SqlIdentifier) operands[0],
                    (SqlNodeList) operands[1], operands[2]);
        }
    }

    private final SqlIdentifier name;

    private final @Nullable SqlNodeList createOptionList;

    private final @Nullable SqlNode storageProfiles;

    /** Creates a SqlCreateZone. */
    public IgniteSqlCreateZoneV2(
            SqlParserPos pos,
            boolean ifNotExists,
            SqlIdentifier name,
            @Nullable SqlNodeList createOptionList,
            @Nullable SqlNode storageProfiles
    ) {
        super(new Operator(ifNotExists), pos, false, ifNotExists);

        this.name = Objects.requireNonNull(name, "name");
        this.createOptionList = createOptionList;
        this.storageProfiles = storageProfiles;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteDdlOperator getOperator() {
        return (IgniteDdlOperator) super.getOperator();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("nullness")
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, createOptionList, storageProfiles);
    }

    /** {@inheritDoc} */
    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("ZONE");
        if (ifNotExists()) {
            writer.keyword("IF NOT EXISTS");
        }

        name.unparse(writer, leftPrec, rightPrec);

        if (createOptionList != null) {
            writer.keyword("(");

            createOptionList.unparse(writer, 0, 0);

            writer.keyword(")");
        }

        if (storageProfiles != null) {
            storageProfiles.unparse(writer, 0, 0);
        }
    }

    /**
     * Get name of the distribution zone.
     */
    public SqlIdentifier name() {
        return name;
    }

    /**
     * Get list of the specified options to create distribution zone with.
     */
    public @Nullable SqlNodeList createOptionList() {
        return createOptionList;
    }

    /**
     * Get list of the specified profiles to create distribution zone with.
     */
    public @Nullable SqlNode storageProfiles() {
        return storageProfiles;
    }

    /**
     * Get whether the IF NOT EXISTS is specified.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }
}
