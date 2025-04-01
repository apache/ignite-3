/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.sql;

import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.STORAGE_PROFILES;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code CREATE ZONE} statement with Ignite specific features.
 */
public class IgniteSqlCreateZone extends SqlCreate {

    /** CREATE ZONE operator. */
    protected static class Operator extends IgniteDdlOperator {

        /** Constructor. */
        protected Operator(boolean existFlag) {
            super("CREATE ZONE", SqlKind.OTHER_DDL, existFlag);
        }

        /** {@inheritDoc} */
        @Override
        public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
                SqlParserPos pos, @Nullable SqlNode... operands) {

            return new IgniteSqlCreateZone(pos, existFlag(), (SqlIdentifier) operands[0],
                    (SqlNodeList) operands[1], (SqlNodeList) operands[2]);
        }
    }

    private final SqlIdentifier name;

    private final @Nullable SqlNodeList createOptionList;

    private final SqlNodeList storageProfiles;

    /** Creates a SqlCreateZone. */
    public static IgniteSqlCreateZone create(
            SqlParserPos pos,
            boolean ifNotExists,
            SqlIdentifier name,
            SqlNodeList createOptionList
    ) {
        SqlNodeList profiles = SqlNodeList.of(pos, new ArrayList<>());

        for (SqlNode c : createOptionList) {
            IgniteSqlZoneOption opt = (IgniteSqlZoneOption) c;
            if (opt.key().getSimple().equals(STORAGE_PROFILES.name())) {
                assert opt.value() instanceof SqlCharStringLiteral : opt.value();

                String profilesStr = ((SqlCharStringLiteral) opt.value()).getValueAs(String.class);
                String[] profilesStrings = profilesStr.split("\\s*,\\s*");

                for (String p : profilesStrings) {
                    if (!p.isBlank()) {
                        profiles.add(SqlLiteral.createCharString(p, null, opt.getParserPosition()));
                    }
                }

                break;
            }
        }

        return new IgniteSqlCreateZone(pos, ifNotExists, name, createOptionList, profiles);
    }

    /** Creates a SqlCreateZone. */
    public IgniteSqlCreateZone(
            SqlParserPos pos,
            boolean ifNotExists,
            SqlIdentifier name,
            @Nullable SqlNodeList createOptionList,
            SqlNodeList storageProfiles
    ) {
        super(new Operator(ifNotExists), pos, false, ifNotExists);

        this.name = Objects.requireNonNull(name, "name");
        this.createOptionList = createOptionList;
        this.storageProfiles = Objects.requireNonNull(storageProfiles, "storageProfiles");
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

        if (createOptionList != null && (createOptionList.size() > 1
                || !((IgniteSqlZoneOption) createOptionList.get(0)).key().getSimple().equals(STORAGE_PROFILES.name()))) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : createOptionList) {
                IgniteSqlZoneOption opt = (IgniteSqlZoneOption) c;
                if (opt.key().getSimple().equals(STORAGE_PROFILES.name())) {
                    continue;
                }
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }

        if (!storageProfiles.isEmpty()) {
            writer.keyword("STORAGE PROFILES");

            SqlWriter.Frame frame = writer.startList(FrameTypeEnum.SIMPLE, "[", "]");
            for (SqlNode c : storageProfiles) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
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
     * Get list of the specified storage profiles options.
     */
    public SqlNodeList storageProfiles() {
        return storageProfiles;
    }

    /**
     * Get whether the IF NOT EXISTS is specified.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }
}
