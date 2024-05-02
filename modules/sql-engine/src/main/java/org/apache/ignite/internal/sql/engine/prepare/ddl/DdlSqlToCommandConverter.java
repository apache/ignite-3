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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.TableOptionEnum.PRIMARY_ZONE;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.TableOptionEnum.STORAGE_PROFILE;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.AFFINITY_FUNCTION;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_AUTO_ADJUST;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_FILTER;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.PARTITIONS;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.REPLICAS;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.STORAGE_PROFILES;
import static org.apache.ignite.internal.sql.engine.util.IgniteMath.convertToByteExact;
import static org.apache.ignite.internal.sql.engine.util.IgniteMath.convertToIntExact;
import static org.apache.ignite.internal.sql.engine.util.IgniteMath.convertToShortExact;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.fromInternal;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalLiteral.IntervalValue;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateIndexCommand.Type;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommandToRemove.PrimaryKeyIndexType;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterTableAddColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterTableDropColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterZoneRenameTo;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterZoneSet;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterZoneSetDefault;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateIndex;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateTable;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateTableOption;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateZone;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropIndex;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropTable;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropZone;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlIndexType;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlPrimaryKeyConstraint;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlPrimaryKeyIndexType;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlZoneOption;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.SchemaNotFoundException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * DdlSqlToCommandConverter.
 */
// TODO: IGNITE-15859 Add documentation
public class DdlSqlToCommandConverter {
    /** Mapping: Table option ID -> DDL option info. */
    private final Map<TableOptionEnum, DdlOptionInfo<CreateTableCommandToRemove, ?>> tableOptionInfos;

    /** Mapping: Zone option ID -> DDL option info. */
    private final Map<ZoneOptionEnum, DdlOptionInfo<CreateZoneCommand, ?>> zoneOptionInfos;

    /** Mapping: Zone option ID -> DDL option info. */
    private final Map<ZoneOptionEnum, DdlOptionInfo<AlterZoneSetCommand, ?>> alterZoneOptionInfos;

    /** Zone options set. */
    private final Set<String> knownZoneOptionNames;

    /**
     * Constructor.
     */
    public DdlSqlToCommandConverter() {
        knownZoneOptionNames = EnumSet.allOf(ZoneOptionEnum.class)
                .stream()
                .map(Enum::name)
                .collect(Collectors.toSet());

        this.tableOptionInfos = new EnumMap<>(Map.of(
                PRIMARY_ZONE, new DdlOptionInfo<>(String.class, null, CreateTableCommandToRemove::zone),
                STORAGE_PROFILE, new DdlOptionInfo<>(String.class, this::checkEmptyString, CreateTableCommandToRemove::storageProfile)
        ));

        // CREATE ZONE options.
        zoneOptionInfos = new EnumMap<>(Map.of(
                REPLICAS, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommand::replicas),
                PARTITIONS, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommand::partitions),
                AFFINITY_FUNCTION, new DdlOptionInfo<>(String.class, null, CreateZoneCommand::affinity),
                DATA_NODES_FILTER, new DdlOptionInfo<>(String.class, null, CreateZoneCommand::nodeFilter),
                DATA_NODES_AUTO_ADJUST,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommand::dataNodesAutoAdjust),
                DATA_NODES_AUTO_ADJUST_SCALE_UP,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommand::dataNodesAutoAdjustScaleUp),
                DATA_NODES_AUTO_ADJUST_SCALE_DOWN,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommand::dataNodesAutoAdjustScaleDown),
                STORAGE_PROFILES, new DdlOptionInfo<>(String.class, this::checkEmptyString, CreateZoneCommand::storageProfiles)
        ));

        // ALTER ZONE options.
        alterZoneOptionInfos = new EnumMap<>(Map.of(
                REPLICAS, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneSetCommand::replicas),
                PARTITIONS, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneSetCommand::partitions),
                DATA_NODES_FILTER, new DdlOptionInfo<>(String.class, null, AlterZoneSetCommand::nodeFilter),
                DATA_NODES_AUTO_ADJUST,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneSetCommand::dataNodesAutoAdjust),
                DATA_NODES_AUTO_ADJUST_SCALE_UP,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneSetCommand::dataNodesAutoAdjustScaleUp),
                DATA_NODES_AUTO_ADJUST_SCALE_DOWN,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneSetCommand::dataNodesAutoAdjustScaleDown)
        ));
    }

    /**
     * Converts a given ddl AST to a ddl command.
     *
     * @param ddlNode Root node of the given AST.
     * @param ctx Planning context.
     */
    public DdlCommand convert(SqlDdl ddlNode, PlanningContext ctx) {
        if (ddlNode instanceof IgniteSqlCreateTable) {
            return convertCreateTable((IgniteSqlCreateTable) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlDropTable) {
            return convertDropTable((IgniteSqlDropTable) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlAlterTableAddColumn) {
            return convertAlterTableAdd((IgniteSqlAlterTableAddColumn) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlAlterTableDropColumn) {
            return convertAlterTableDrop((IgniteSqlAlterTableDropColumn) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlAlterColumn) {
            return convertAlterColumn((IgniteSqlAlterColumn) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlCreateIndex) {
            return convertAddIndex((IgniteSqlCreateIndex) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlDropIndex) {
            return convertDropIndex((IgniteSqlDropIndex) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlCreateZone) {
            return convertCreateZone((IgniteSqlCreateZone) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlAlterZoneRenameTo) {
            return convertAlterZoneRename((IgniteSqlAlterZoneRenameTo) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlAlterZoneSet) {
            return convertAlterZoneSet((IgniteSqlAlterZoneSet) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlAlterZoneSetDefault) {
            return convertAlterZoneSetDefault((IgniteSqlAlterZoneSetDefault) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlDropZone) {
            return convertDropZone((IgniteSqlDropZone) ddlNode, ctx);
        }

        throw new SqlException(STMT_VALIDATION_ERR, "Unsupported operation ["
                + "sqlNodeKind=" + ddlNode.getKind() + "; "
                + "querySql=\"" + ctx.query() + "\"]");
    }

    /**
     * Converts a given CreateTable AST to a CreateTable command.
     *
     * @param createTblNode Root node of the given AST.
     * @param ctx Planning context.
     */
    private CreateTableCommandToRemove convertCreateTable(IgniteSqlCreateTable createTblNode, PlanningContext ctx) {
        CreateTableCommandToRemove createTblCmd = new CreateTableCommandToRemove();

        createTblCmd.schemaName(deriveSchemaName(createTblNode.name(), ctx));
        createTblCmd.tableName(deriveObjectName(createTblNode.name(), ctx, "tableName"));
        createTblCmd.ifTableExists(createTblNode.ifNotExists());

        if (createTblNode.createOptionList() != null) {

            for (SqlNode optionNode : createTblNode.createOptionList().getList()) {
                IgniteSqlCreateTableOption option = (IgniteSqlCreateTableOption) optionNode;

                assert option.key().isSimple() : option.key();

                String optionKey = option.key().getSimple().toUpperCase();

                try {
                    DdlOptionInfo<CreateTableCommandToRemove, ?> tblOptionInfo = tableOptionInfos.get(TableOptionEnum.valueOf(optionKey));

                    updateCommandOption("Table", optionKey, option.value(), tblOptionInfo, ctx.query(), createTblCmd);
                } catch (IllegalArgumentException ignored) {
                    throw new SqlException(
                            STMT_VALIDATION_ERR, String.format("Unexpected table option [option=%s, query=%s]", optionKey, ctx.query()));
                }
            }
        }

        List<IgniteSqlPrimaryKeyConstraint> pkConstraints = createTblNode.columnList().getList().stream()
                .filter(IgniteSqlPrimaryKeyConstraint.class::isInstance)
                .map(IgniteSqlPrimaryKeyConstraint.class::cast)
                .collect(Collectors.toList());

        if (pkConstraints.isEmpty() && Commons.implicitPkEnabled()) {
            SqlIdentifier colName = new SqlIdentifier(Commons.IMPLICIT_PK_COL_NAME, SqlParserPos.ZERO);

            pkConstraints.add(new IgniteSqlPrimaryKeyConstraint(SqlParserPos.ZERO, null, SqlNodeList.of(colName),
                    IgniteSqlPrimaryKeyIndexType.IMPLICIT_HASH));

            SqlDataTypeSpec type = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, SqlParserPos.ZERO), SqlParserPos.ZERO);
            SqlNode col = SqlDdlNodes.column(SqlParserPos.ZERO, colName, type, null, ColumnStrategy.DEFAULT);

            createTblNode.columnList().add(0, col);
        }

        if (nullOrEmpty(pkConstraints)) {
            throw new SqlException(STMT_VALIDATION_ERR, "Table without PRIMARY KEY is not supported");
        } else if (pkConstraints.size() > 1) {
            throw new SqlException(STMT_VALIDATION_ERR, "Unexpected number of primary key constraints ["
                    + "expected at most one, but was " + pkConstraints.size() + "; "
                    + "querySql=\"" + ctx.query() + "\"]");
        }

        IgniteSqlPrimaryKeyConstraint pkConstraint = pkConstraints.get(0);
        SqlNodeList columnNodes = pkConstraint.getColumnList();

        List<String> pkColumns = new ArrayList<>(columnNodes.size());
        List<Collation> pkCollations = new ArrayList<>(columnNodes.size());

        PrimaryKeyIndexType pkIndexType = convertPrimaryIndexType(pkConstraint.getIndexType());
        boolean supportCollation = pkIndexType == PrimaryKeyIndexType.SORTED;

        parseColumnList(pkConstraint.getColumnList(), pkColumns, pkCollations, supportCollation);

        createTblCmd.primaryIndexType(pkIndexType);
        createTblCmd.primaryKeyColumns(pkColumns);
        createTblCmd.primaryKeyCollations(pkCollations);

        List<String> colocationCols = createTblNode.colocationColumns() == null
                ? null
                : createTblNode.colocationColumns().getList().stream()
                        .map(SqlIdentifier.class::cast)
                        .map(SqlIdentifier::getSimple)
                        .collect(Collectors.toList());

        createTblCmd.colocationColumns(colocationCols);

        List<SqlColumnDeclaration> colDeclarations = createTblNode.columnList().getList().stream()
                .filter(SqlColumnDeclaration.class::isInstance)
                .map(SqlColumnDeclaration.class::cast)
                .collect(Collectors.toList());

        IgnitePlanner planner = ctx.planner();

        List<ColumnDefinition> cols = new ArrayList<>(colDeclarations.size());

        for (SqlColumnDeclaration col : colDeclarations) {
            if (!col.name.isSimple()) {
                throw new SqlException(STMT_VALIDATION_ERR, "Unexpected value of columnName ["
                        + "expected a simple identifier, but was " + col.name + "; "
                        + "querySql=\"" + ctx.query() + "\"]");
            }

            String name = col.name.getSimple();

            RelDataType relType = planner.convert(col.dataType, !pkColumns.contains(name));

            DefaultValueDefinition dflt = convertDefault(col.expression, relType, name);
            cols.add(new ColumnDefinition(name, relType, dflt));
        }

        createTblCmd.columns(cols);

        return createTblCmd;
    }

    /**
     * Converts a given IgniteSqlAlterTableAddColumn AST to a AlterTableAddCommand.
     *
     * @param alterTblNode Root node of the given AST.
     * @param ctx Planning context.
     */
    private AlterTableAddCommand convertAlterTableAdd(IgniteSqlAlterTableAddColumn alterTblNode, PlanningContext ctx) {
        AlterTableAddCommand alterTblCmd = new AlterTableAddCommand();

        alterTblCmd.schemaName(deriveSchemaName(alterTblNode.name(), ctx));
        alterTblCmd.tableName(deriveObjectName(alterTblNode.name(), ctx, "table name"));
        alterTblCmd.ifTableExists(alterTblNode.ifExists());

        List<ColumnDefinition> cols = new ArrayList<>(alterTblNode.columns().size());

        for (SqlNode colNode : alterTblNode.columns()) {
            assert colNode instanceof SqlColumnDeclaration : colNode.getClass();

            SqlColumnDeclaration col = (SqlColumnDeclaration) colNode;

            assert col.name.isSimple();

            Boolean nullable = col.dataType.getNullable();
            RelDataType relType = ctx.planner().convert(col.dataType, nullable != null ? nullable : true);
            String name = col.name.getSimple();
            DefaultValueDefinition dflt = convertDefault(col.expression, relType, name);

            cols.add(new ColumnDefinition(name, relType, dflt));
        }

        alterTblCmd.columns(cols);

        return alterTblCmd;
    }

    private static DefaultValueDefinition convertDefault(@Nullable SqlNode expression, RelDataType relType, String name) {
        if (expression == null) {
            return DefaultValueDefinition.constant(null);
        } else if (expression instanceof SqlIdentifier) {
            return DefaultValueDefinition.functionCall(((SqlIdentifier) expression).getSimple());
        } else if (expression instanceof SqlLiteral) {
            ColumnType columnType = TypeUtils.columnType(relType);
            assert columnType != null : "RelType to columnType conversion should not return null";

            Object val = fromLiteral(columnType, name, (SqlLiteral) expression, relType.getPrecision(), relType.getScale());
            return DefaultValueDefinition.constant(val);
        } else {
            throw new IllegalArgumentException("Unsupported default expression: " + expression.getKind());
        }
    }

    private AlterColumnCommand convertAlterColumn(IgniteSqlAlterColumn alterColumnNode, PlanningContext ctx) {
        AlterColumnCommand cmd = new AlterColumnCommand();

        cmd.schemaName(deriveSchemaName(alterColumnNode.name(), ctx));
        cmd.tableName(deriveObjectName(alterColumnNode.name(), ctx, "table name"));
        cmd.ifTableExists(alterColumnNode.ifExists());
        cmd.columnName(alterColumnNode.columnName().getSimple());

        if (alterColumnNode.dataType() != null) {
            cmd.type(ctx.planner().convert(alterColumnNode.dataType(), true));
        }

        if (alterColumnNode.notNull() != null) {
            cmd.notNull(alterColumnNode.notNull());
        }

        if (alterColumnNode.expression() != null) {
            SqlNode expr = alterColumnNode.expression();

            Function<ColumnType, DefaultValue> resolveDfltFunc;

            @Nullable RelDataType relType = cmd.type();

            int precision = relType == null ? PRECISION_NOT_SPECIFIED : relType.getPrecision();
            int scale = relType == null ? SCALE_NOT_SPECIFIED : relType.getScale();
            String name = alterColumnNode.columnName().getSimple();

            if (expr instanceof SqlLiteral) {
                resolveDfltFunc = type -> DefaultValue.constant(fromLiteral(type, name, (SqlLiteral) expr, precision, scale));
            } else {
                throw new IllegalStateException("Invalid expression type " + expr.getKind());
            }

            cmd.defaultValueResolver(resolveDfltFunc);
        }

        return cmd;
    }

    /**
     * Converts a given IgniteSqlAlterTableDropColumn AST to a AlterTableDropCommand.
     *
     * @param alterTblNode Root node of the given AST.
     * @param ctx Planning context.
     */
    private AlterTableDropCommand convertAlterTableDrop(IgniteSqlAlterTableDropColumn alterTblNode, PlanningContext ctx) {
        AlterTableDropCommand alterTblCmd = new AlterTableDropCommand();

        alterTblCmd.schemaName(deriveSchemaName(alterTblNode.name(), ctx));
        alterTblCmd.tableName(deriveObjectName(alterTblNode.name(), ctx, "table name"));
        alterTblCmd.ifTableExists(alterTblNode.ifExists());

        Set<String> cols = new HashSet<>(alterTblNode.columns().size());
        alterTblNode.columns().forEach(c -> cols.add(((SqlIdentifier) c).getSimple()));

        alterTblCmd.columns(cols);

        return alterTblCmd;
    }

    /**
     * Converts a given DropTable AST to a DropTable command.
     *
     * @param dropTblNode Root node of the given AST.
     * @param ctx Planning context.
     */
    private DropTableCommand convertDropTable(IgniteSqlDropTable dropTblNode, PlanningContext ctx) {
        DropTableCommand dropTblCmd = new DropTableCommand();

        dropTblCmd.schemaName(deriveSchemaName(dropTblNode.name(), ctx));
        dropTblCmd.tableName(deriveObjectName(dropTblNode.name(), ctx, "tableName"));
        dropTblCmd.ifTableExists(dropTblNode.ifExists);

        return dropTblCmd;
    }

    /**
     * Converts create index to appropriate wrapper.
     */
    private CreateIndexCommand convertAddIndex(IgniteSqlCreateIndex sqlCmd, PlanningContext ctx) {
        CreateIndexCommand createIdxCmd = new CreateIndexCommand();

        createIdxCmd.schemaName(deriveSchemaName(sqlCmd.tableName(), ctx));
        createIdxCmd.tableName(deriveObjectName(sqlCmd.tableName(), ctx, "table name"));
        createIdxCmd.indexName(sqlCmd.indexName().getSimple());
        createIdxCmd.type(convertIndexType(sqlCmd.type()));

        SqlNodeList columnList = sqlCmd.columnList();
        List<String> columns = new ArrayList<>(columnList.size());
        List<Collation> collations = new ArrayList<>(columnList.size());
        boolean supportCollation = createIdxCmd.type() == Type.SORTED;

        parseColumnList(columnList, columns, collations, supportCollation);

        createIdxCmd.columns(columns);
        createIdxCmd.collations(collations);

        createIdxCmd.ifNotExists(sqlCmd.ifNotExists());

        return createIdxCmd;
    }

    private static void parseColumnList(
            SqlNodeList columnList,
            List<String> columns,
            List<Collation> collations,
            boolean supportCollation
    ) {
        for (SqlNode col : columnList.getList()) {
            boolean desc = false;

            if (col.getKind() == SqlKind.DESCENDING) {
                col = ((SqlCall) col).getOperandList().get(0);

                desc = true;
            }

            String columnName = ((SqlIdentifier) col).getSimple();
            columns.add(columnName);
            if (supportCollation) {
                collations.add(desc ? Collation.DESC_NULLS_FIRST : Collation.ASC_NULLS_LAST);
            }
        }
    }

    /**
     * Converts drop index to appropriate wrapper.
     */
    private DropIndexCommand convertDropIndex(IgniteSqlDropIndex sqlCmd, PlanningContext ctx) {
        DropIndexCommand dropCmd = new DropIndexCommand();

        String schemaName = deriveSchemaName(sqlCmd.indexName(), ctx);
        String indexName = deriveObjectName(sqlCmd.indexName(), ctx, "index name");

        dropCmd.schemaName(schemaName);
        dropCmd.indexName(indexName);
        dropCmd.ifNotExists(sqlCmd.ifExists());

        return dropCmd;
    }

    /**
     * Converts a given CreateZone AST to a CreateZone command.
     *
     * @param createZoneNode Root node of the given AST.
     * @param ctx Planning context.
     */
    private CreateZoneCommand convertCreateZone(IgniteSqlCreateZone createZoneNode, PlanningContext ctx) {
        CreateZoneCommand createZoneCmd = new CreateZoneCommand();

        createZoneCmd.schemaName(deriveSchemaName(createZoneNode.name(), ctx));
        createZoneCmd.zoneName(deriveObjectName(createZoneNode.name(), ctx, "zoneName"));
        createZoneCmd.ifNotExists(createZoneNode.ifNotExists());

        if (createZoneNode.createOptionList() == null) {
            throw new SqlException(STMT_VALIDATION_ERR, STORAGE_PROFILES + " option cannot be null");
        }

        Set<String> remainingKnownOptions = new HashSet<>(knownZoneOptionNames);

        for (SqlNode optionNode : createZoneNode.createOptionList().getList()) {
            IgniteSqlZoneOption option = (IgniteSqlZoneOption) optionNode;

            assert option.key().isSimple() : option.key();

            String optionName = option.key().getSimple().toUpperCase();

            DdlOptionInfo<CreateZoneCommand, ?> zoneOptionInfo = null;

            if (remainingKnownOptions.remove(optionName)) {
                zoneOptionInfo = zoneOptionInfos.get(ZoneOptionEnum.valueOf(optionName));
            } else if (knownZoneOptionNames.contains(optionName)) {
                throw duplicateZoneOption(ctx, optionName);
            }

            if (zoneOptionInfo == null) {
                throw unexpectedZoneOption(ctx, optionName);
            }

            updateCommandOption("Zone", optionName, (SqlLiteral) option.value(), zoneOptionInfo, ctx.query(), createZoneCmd);
        }

        if (createZoneCmd.storageProfiles() == null) {
            throw new SqlException(STMT_VALIDATION_ERR, STORAGE_PROFILES + " option cannot be null");
        }

        return createZoneCmd;
    }


    /**
     * Converts the given IgniteSqlAlterZoneSet AST node to a AlterZoneCommand.
     *
     * @param alterZoneSet Root node of the given AST.
     * @param ctx Planning context.
     */
    private DdlCommand convertAlterZoneSet(IgniteSqlAlterZoneSet alterZoneSet, PlanningContext ctx) {
        AlterZoneSetCommand alterZoneCmd = new AlterZoneSetCommand();

        alterZoneCmd.schemaName(deriveSchemaName(alterZoneSet.name(), ctx));
        alterZoneCmd.zoneName(deriveObjectName(alterZoneSet.name(), ctx, "zoneName"));
        alterZoneCmd.ifExists(alterZoneSet.ifExists());

        Set<String> remainingKnownOptions = new HashSet<>(knownZoneOptionNames);

        for (SqlNode optionNode : alterZoneSet.alterOptionsList().getList()) {
            IgniteSqlZoneOption option = (IgniteSqlZoneOption) optionNode;
            String optionName = option.key().getSimple().toUpperCase();

            if (!knownZoneOptionNames.contains(optionName)) {
                throw unexpectedZoneOption(ctx, optionName);
            } else if (!remainingKnownOptions.remove(optionName)) {
                throw duplicateZoneOption(ctx, optionName);
            }

            DdlOptionInfo<AlterZoneSetCommand, ?> zoneOptionInfo = alterZoneOptionInfos.get(ZoneOptionEnum.valueOf(optionName));

            assert zoneOptionInfo != null : optionName;
            assert option.value() instanceof SqlLiteral : option.value();

            updateCommandOption("Zone", optionName, (SqlLiteral) option.value(), zoneOptionInfo, ctx.query(), alterZoneCmd);
        }

        return alterZoneCmd;
    }

    /**
     * Converts the given {@link IgniteSqlAlterZoneSetDefault} AST node to a {@link AlterZoneSetDefaultCommand}.
     *
     * @param alterZoneSetDefault Root node of the given AST.
     * @param ctx Planning context.
     */
    private DdlCommand convertAlterZoneSetDefault(IgniteSqlAlterZoneSetDefault alterZoneSetDefault, PlanningContext ctx) {
        AlterZoneSetDefaultCommand cmd = new AlterZoneSetDefaultCommand();

        cmd.schemaName(deriveSchemaName(alterZoneSetDefault.name(), ctx));
        cmd.zoneName(deriveObjectName(alterZoneSetDefault.name(), ctx, "zoneName"));
        cmd.ifExists(alterZoneSetDefault.ifExists());

        return cmd;
    }

    /**
     * Converts the given IgniteSqlAlterZoneRenameTo AST node to a AlterZoneCommand.
     *
     * @param alterZoneRename Root node of the given AST.
     * @param ctx Planning context.
     */
    private DdlCommand convertAlterZoneRename(IgniteSqlAlterZoneRenameTo alterZoneRename, PlanningContext ctx) {
        AlterZoneRenameCommand cmd = new AlterZoneRenameCommand();

        cmd.schemaName(deriveSchemaName(alterZoneRename.name(), ctx));
        cmd.zoneName(deriveObjectName(alterZoneRename.name(), ctx, "zoneName"));
        cmd.newZoneName(alterZoneRename.newName().getSimple());
        cmd.ifExists(alterZoneRename.ifExists());

        return cmd;
    }

    /**
     * Converts a given DropZone AST to a DropZone command.
     *
     * @param dropZoneNode Root node of the given AST.
     * @param ctx Planning context.
     */
    private DropZoneCommandToRemove convertDropZone(IgniteSqlDropZone dropZoneNode, PlanningContext ctx) {
        DropZoneCommandToRemove dropZoneCmd = new DropZoneCommandToRemove();

        dropZoneCmd.schemaName(deriveSchemaName(dropZoneNode.name(), ctx));
        dropZoneCmd.zoneName(deriveObjectName(dropZoneNode.name(), ctx, "zoneName"));
        dropZoneCmd.ifExists(dropZoneNode.ifExists());

        return dropZoneCmd;
    }

    /** Derives a schema name from the compound identifier. */
    private String deriveSchemaName(SqlIdentifier id, PlanningContext ctx) {
        String schemaName;
        if (id.isSimple()) {
            schemaName = ctx.schemaName();
        } else {
            SqlIdentifier schemaId = id.skipLast(1);

            if (!schemaId.isSimple()) {
                throw new SqlException(STMT_VALIDATION_ERR, "Unexpected value of schemaName ["
                        + "expected a simple identifier, but was " + schemaId + "; "
                        + "querySql=\"" + ctx.query() + "\"]");
            }

            schemaName = schemaId.getSimple();
        }

        ensureSchemaExists(ctx, schemaName);

        return schemaName;
    }

    /** Derives an object(a table, an index, etc) name from the compound identifier. */
    private String deriveObjectName(SqlIdentifier id, PlanningContext ctx, String objDesc) {
        if (id.isSimple()) {
            return id.getSimple();
        }

        SqlIdentifier objId = id.getComponent(id.skipLast(1).names.size());

        if (!objId.isSimple()) {
            throw new SqlException(STMT_VALIDATION_ERR, "Unexpected value of " + objDesc + " ["
                    + "expected a simple identifier, but was " + objId + "; "
                    + "querySql=\"" + ctx.query() + "\"]");
        }

        return objId.getSimple();
    }

    private void ensureSchemaExists(PlanningContext ctx, String schemaName) {
        if (ctx.catalogReader().getRootSchema().getSubSchema(schemaName, true) == null) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    private <S, T> void updateCommandOption(
            String sqlObjName,
            Object optId,
            SqlNode value,
            DdlOptionInfo<S, T> optInfo,
            String query,
            S target
    ) {
        T expectedValue = extractValueForUpdateCommandOption(sqlObjName, optId, value, optInfo, query);
        validateValue(sqlObjName, optId, optInfo, query, expectedValue);
        optInfo.setter.accept(target, expectedValue);
    }

    private static <T, S> T extractValueForUpdateCommandOption(
            String sqlObjName,
            Object optId,
            SqlNode value,
            DdlOptionInfo<S, T> optInfo,
            String query
    ) {
        SqlKind valueKind = value.getKind();
        switch (valueKind) {
            case IDENTIFIER:
                return (T) ((SqlIdentifier) value).getSimple();
            case LITERAL:
                return valueFromLiteralAccordingToOptionType(sqlObjName, optId, optInfo, query, (SqlLiteral) value);
            default:
                String msg = String.format(
                        "Invalid %s value kind [kind=%s, expectedKind=(IDENTIFIER, LITERAL), query=%s]",
                        sqlObjName.toLowerCase(),
                        valueKind,
                        query);
                throw new SqlException(STMT_VALIDATION_ERR, msg);
        }
    }

    private static <S, T> void validateValue(String sqlObjName, Object optId, DdlOptionInfo<S, T> optInfo, String query, T expectedValue) {
        if (optInfo.validator == null) {
            return;
        }

        try {
            optInfo.validator.accept(expectedValue);
        } catch (Throwable e) {
            String msg = String.format(
                    "%s option validation failed [option=%s, err=%s, query=%s]",
                    sqlObjName,
                    optId,
                    e.getMessage(),
                    query);
            throw new SqlException(STMT_VALIDATION_ERR, msg, e);
        }
    }

    private static <T, S> T valueFromLiteralAccordingToOptionType(
            String sqlObjName,
            Object optId,
            DdlOptionInfo<S, T> optInfo,
            String query,
            SqlLiteral literalValue) {
        try {
            return literalValue.getValueAs(optInfo.type);
        } catch (Throwable cause) {
            String msg = String.format(
                    "Invalid %s option type [option=%s, expectedType=%s, query=%s]",
                    sqlObjName.toLowerCase(),
                    optId,
                    optInfo.type.getSimpleName(),
                    query);
            throw new SqlException(STMT_VALIDATION_ERR, msg, cause);
        }
    }

    private void checkPositiveNumber(int num) {
        if (num < 0) {
            throw new SqlException(STMT_VALIDATION_ERR, "Must be positive:" + num);
        }
    }

    private void checkEmptyString(String string) {
        if (string.isEmpty()) {
            throw new SqlException(STMT_VALIDATION_ERR, "String cannot be empty");
        }
    }

    private Type convertIndexType(IgniteSqlIndexType type) {
        switch (type) {
            case SORTED:
            case IMPLICIT_SORTED:
                return Type.SORTED;
            case HASH:
                return Type.HASH;
            default:
                throw new AssertionError("Unknown index type [type=" + type + "]");
        }
    }

    private PrimaryKeyIndexType convertPrimaryIndexType(IgniteSqlPrimaryKeyIndexType type) {
        switch (type) {
            case SORTED:
                return PrimaryKeyIndexType.SORTED;
            case HASH:
            case IMPLICIT_HASH:
                return PrimaryKeyIndexType.HASH;
            default:
                throw new AssertionError("Unknown index type [type=" + type + "]");
        }
    }

    /**
     * Creates a value of required type from the literal.
     */
    private static @Nullable Object fromLiteral(ColumnType columnType, String name, SqlLiteral literal, int precision, int scale) {
        if (literal.getValue() == null) {
            return null;
        }

        try {
            switch (columnType) {
                case PERIOD: {
                    if (!(literal instanceof SqlIntervalLiteral)) {
                        throw new SqlException(STMT_VALIDATION_ERR,
                                "Default expression is not belongs to interval type");
                    }

                    String strValue = Objects.requireNonNull(literal.toValue());
                    SqlNumericLiteral numLiteral = SqlLiteral.createExactNumeric(strValue, literal.getParserPosition());
                    int val = numLiteral.intValue(true);
                    SqlIntervalLiteral literal0 = (SqlIntervalLiteral) literal;
                    SqlIntervalQualifier qualifier = ((IntervalValue) literal0.getValue()).getIntervalQualifier();
                    if (qualifier.typeName() == SqlTypeName.INTERVAL_YEAR) {
                        val = val * 12;
                    }
                    return fromInternal(val, Period.class);
                }
                case DURATION: {
                    if (!(literal instanceof SqlIntervalLiteral)) {
                        throw new SqlException(STMT_VALIDATION_ERR,
                                "Default expression is not belongs to interval type");
                    }
                    String strValue = Objects.requireNonNull(literal.toValue());
                    SqlNumericLiteral numLiteral = SqlLiteral.createExactNumeric(strValue, literal.getParserPosition());
                    long val = numLiteral.longValue(true);
                    SqlIntervalLiteral literal0 = (SqlIntervalLiteral) literal;
                    SqlIntervalQualifier qualifier = ((IntervalValue) literal0.getValue()).getIntervalQualifier();
                    if (qualifier.typeName() == SqlTypeName.INTERVAL_DAY) {
                        val = Duration.ofDays(val).toMillis();
                    } else if (qualifier.typeName() == SqlTypeName.INTERVAL_HOUR) {
                        val = Duration.ofHours(val).toMillis();
                    } else if (qualifier.typeName() == SqlTypeName.INTERVAL_MINUTE) {
                        val = Duration.ofMinutes(val).toMillis();
                    } else if (qualifier.typeName() == SqlTypeName.INTERVAL_SECOND) {
                        val = Duration.ofSeconds(val).toMillis();
                    }
                    return fromInternal(val, Duration.class);
                }
                case STRING: {
                    String val = literal.toValue();
                    // varchar without limitation
                    if (precision != PRECISION_NOT_SPECIFIED && Objects.requireNonNull(val).length() > precision) {
                        throw new SqlException(STMT_VALIDATION_ERR,
                                format("Value too long for type character({})", precision));
                    }
                    return val;
                }
                case UUID:
                    return UUID.fromString(Objects.requireNonNull(literal.toValue()));
                case DATE: {
                    try {
                        literal = SqlParserUtil.parseDateLiteral(literal.getValueAs(String.class), literal.getParserPosition());
                        int val = literal.getValueAs(DateString.class).getDaysSinceEpoch();
                        return fromInternal(val, LocalDate.class);
                    } catch (CalciteContextException e) {
                        literal = SqlParserUtil.parseTimestampLiteral(literal.getValueAs(String.class), literal.getParserPosition());
                        TimestampString tsString = literal.getValueAs(TimestampString.class);
                        int val = convertToIntExact(TimeUnit.MILLISECONDS.toDays(tsString.getMillisSinceEpoch()));
                        return fromInternal(val, LocalDate.class);
                    }
                }
                case TIME: {
                    String strLiteral = literal.getValueAs(String.class).trim();
                    int pos = strLiteral.indexOf(' ');
                    if (pos != -1) {
                        strLiteral = strLiteral.substring(pos);
                    }
                    literal = SqlParserUtil.parseTimeLiteral(strLiteral, literal.getParserPosition());
                    int val = literal.getValueAs(TimeString.class).getMillisOfDay();
                    return fromInternal(val, LocalTime.class);
                }
                case DATETIME: {
                    literal = SqlParserUtil.parseTimestampLiteral(literal.getValueAs(String.class), literal.getParserPosition());
                    var tsString = literal.getValueAs(TimestampString.class);

                    return fromInternal(tsString.getMillisSinceEpoch(), LocalDateTime.class);
                }
                case TIMESTAMP:
                    // TODO: IGNITE-17376
                    throw new UnsupportedOperationException("Type is not supported: " + columnType);
                case INT32: {
                    acceptNumericLiteral(literal, columnType);
                    long val = literal.longValue(true);
                    return convertToIntExact(val);
                }
                case INT64: {
                    acceptNumericLiteral(literal, columnType);
                    BigDecimal val = literal.bigDecimalValue();
                    return Objects.requireNonNull(val).longValueExact();
                }
                case INT16: {
                    acceptNumericLiteral(literal, columnType);
                    long val = literal.longValue(true);
                    return convertToShortExact(val);
                }
                case INT8: {
                    acceptNumericLiteral(literal, columnType);
                    long val = literal.longValue(true);
                    return convertToByteExact(val);
                }
                case DECIMAL:
                    acceptNumericLiteral(literal, columnType);
                    BigDecimal val = literal.getValueAs(BigDecimal.class);
                    val = val.setScale(scale, RoundingMode.HALF_UP);
                    if (val.precision() > precision) {
                        throw new SqlException(STMT_VALIDATION_ERR, format("Numeric field overflow for type decimal({}, {})",
                                precision, scale));
                    }
                    return val;
                case DOUBLE:
                    acceptNumericLiteral(literal, columnType);
                    return literal.getValueAs(Double.class);
                case FLOAT:
                    acceptNumericLiteral(literal, columnType);
                    return literal.getValueAs(Float.class);
                case BYTE_ARRAY:
                    byte[] arr = literal.getValueAs(byte[].class);
                    // varbinary without limitation
                    if (precision != PRECISION_NOT_SPECIFIED && Objects.requireNonNull(arr).length > precision) {
                        throw new SqlException(STMT_VALIDATION_ERR,
                                format("Value too long for type binary({})", precision));
                    }
                    return arr;
                case BOOLEAN:
                    return literal.getValueAs(Boolean.class);
                default:
                    throw new IllegalStateException("Unknown type [type=" + columnType + ']');
            }
        } catch (Throwable th) {
            // catch throwable here because literal throws an AssertionError when unable to cast value to a given class
            throw new SqlException(STMT_VALIDATION_ERR, format("Invalid default value for column '{}'", name), th);
        }
    }

    private static void acceptNumericLiteral(SqlLiteral literal, ColumnType columnType) {
        if (!(literal instanceof SqlNumericLiteral)) {
            throw new SqlException(STMT_VALIDATION_ERR, "Default expression can`t be applied to type " + columnType);
        }
    }

    private static IgniteException unexpectedZoneOption(PlanningContext ctx, String optionName) {
        return new SqlException(STMT_VALIDATION_ERR,
                String.format("Unexpected zone option [option=%s, query=%s]", optionName, ctx.query()));
    }

    private static IgniteException duplicateZoneOption(PlanningContext ctx, String optionName) {
        return new SqlException(STMT_VALIDATION_ERR,
                String.format("Duplicate zone option has been specified [option=%s, query=%s]", optionName, ctx.query()));
    }
}
