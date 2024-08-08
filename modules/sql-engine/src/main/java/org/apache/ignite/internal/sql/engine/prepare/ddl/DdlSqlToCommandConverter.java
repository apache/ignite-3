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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultLength;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseStorageProfiles;
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
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.columnType;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.fromInternal;
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
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.ColumnStrategy;
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
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnCommandBuilder;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableAlterColumnCommandBuilder;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnCommandBuilder;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.AlterZoneSetDefaultCommand;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DeferredDefaultValue;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.commands.TablePrimaryKey;
import org.apache.ignite.internal.catalog.commands.TableSortedPrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
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
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlTypeNameSpec;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlZoneOption;
import org.apache.ignite.internal.sql.engine.type.UuidType;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.SchemaNotFoundException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Converts the DDL AST tree to the appropriate catalog command.
 */
public class DdlSqlToCommandConverter {
    /** Mapping: Table option ID -> DDL option info. */
    private final Map<TableOptionEnum, DdlOptionInfo<CreateTableCommandBuilder, ?>> tableOptionInfos;

    /** Mapping: Zone option ID -> DDL option info. */
    private final Map<ZoneOptionEnum, DdlOptionInfo<CreateZoneCommandBuilder, ?>> zoneOptionInfos;

    /** Mapping: Zone option ID -> DDL option info. */
    private final Map<ZoneOptionEnum, DdlOptionInfo<AlterZoneCommandBuilder, ?>> alterZoneOptionInfos;

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
                PRIMARY_ZONE, new DdlOptionInfo<>(String.class, null, CreateTableCommandBuilder::zone),
                STORAGE_PROFILE, new DdlOptionInfo<>(String.class, this::checkEmptyString, CreateTableCommandBuilder::storageProfile)
        ));

        // CREATE ZONE options.
        zoneOptionInfos = new EnumMap<>(Map.of(
                REPLICAS, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommandBuilder::replicas),
                PARTITIONS, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommandBuilder::partitions),
                // TODO https://issues.apache.org/jira/browse/IGNITE-22162 appropriate setter method should be used.
                AFFINITY_FUNCTION, new DdlOptionInfo<>(String.class, null, (builder, params) -> {}),
                DATA_NODES_FILTER, new DdlOptionInfo<>(String.class, null, CreateZoneCommandBuilder::filter),
                DATA_NODES_AUTO_ADJUST,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommandBuilder::dataNodesAutoAdjust),
                DATA_NODES_AUTO_ADJUST_SCALE_UP,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommandBuilder::dataNodesAutoAdjustScaleUp),
                DATA_NODES_AUTO_ADJUST_SCALE_DOWN,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommandBuilder::dataNodesAutoAdjustScaleDown),
                STORAGE_PROFILES, new DdlOptionInfo<>(String.class, this::checkEmptyString,
                        (builder, params) -> builder.storageProfilesParams(parseStorageProfiles(params)))
        ));

        // ALTER ZONE options.
        alterZoneOptionInfos = new EnumMap<>(Map.of(
                REPLICAS, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneCommandBuilder::replicas),
                PARTITIONS, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneCommandBuilder::partitions),
                DATA_NODES_FILTER, new DdlOptionInfo<>(String.class, null, AlterZoneCommandBuilder::filter),
                DATA_NODES_AUTO_ADJUST,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneCommandBuilder::dataNodesAutoAdjust),
                DATA_NODES_AUTO_ADJUST_SCALE_UP,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneCommandBuilder::dataNodesAutoAdjustScaleUp),
                DATA_NODES_AUTO_ADJUST_SCALE_DOWN,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneCommandBuilder::dataNodesAutoAdjustScaleDown)
        ));
    }

    /**
     * Converts a given ddl AST to a catalog command.
     *
     * @param ddlNode Root node of the given AST.
     * @param ctx Planning context.
     * @return Catalog command.
     */
    public CatalogCommand convert(SqlDdl ddlNode, PlanningContext ctx) {
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
     * Converts the given '{@code CREATE TABLE}' AST to the {@link CreateTableCommand} catalog command.
     */
    private CatalogCommand convertCreateTable(IgniteSqlCreateTable createTblNode, PlanningContext ctx) {
        CreateTableCommandBuilder tblBuilder = CreateTableCommand.builder();

        if (createTblNode.createOptionList() != null) {
            for (SqlNode optionNode : createTblNode.createOptionList().getList()) {
                IgniteSqlCreateTableOption option = (IgniteSqlCreateTableOption) optionNode;

                assert option.key().isSimple() : option.key();

                String optionKey = option.key().getSimple().toUpperCase();

                try {
                    DdlOptionInfo<CreateTableCommandBuilder, ?> tblOptionInfo = tableOptionInfos.get(TableOptionEnum.valueOf(optionKey));

                    updateCommandOption("Table", optionKey, option.value(), tblOptionInfo, ctx.query(), tblBuilder);
                } catch (IllegalArgumentException ignored) {
                    throw new SqlException(
                            STMT_VALIDATION_ERR, format("Unexpected table option [option={}, query={}]", optionKey, ctx.query()));
                }
            }
        }

        List<IgniteSqlPrimaryKeyConstraint> pkConstraints = createTblNode.columnList().getList().stream()
                .filter(IgniteSqlPrimaryKeyConstraint.class::isInstance)
                .map(IgniteSqlPrimaryKeyConstraint.class::cast)
                .collect(Collectors.toList());

        for (SqlNode sqlNode : createTblNode.columnList().getList()) {
            if (sqlNode instanceof SqlColumnDeclaration) {
                String colName = ((SqlColumnDeclaration) sqlNode).name.getSimple();

                if (IgniteSqlValidator.isSystemFieldName(colName)) {
                    throw new SqlException(STMT_VALIDATION_ERR, "Failed to validate query. "
                            + "Column '" + colName + "' is reserved name.");
                }
            }
        }

        if (pkConstraints.isEmpty() && Commons.implicitPkEnabled()) {
            SqlIdentifier colName = new SqlIdentifier(Commons.IMPLICIT_PK_COL_NAME, SqlParserPos.ZERO);

            pkConstraints.add(new IgniteSqlPrimaryKeyConstraint(SqlParserPos.ZERO, null, SqlNodeList.of(colName),
                    IgniteSqlPrimaryKeyIndexType.IMPLICIT_HASH));

            SqlIdentifier uuidTypeName = new SqlIdentifier(UuidType.NAME, SqlParserPos.ZERO);
            SqlDataTypeSpec type = new SqlDataTypeSpec(new IgniteSqlTypeNameSpec(uuidTypeName, SqlParserPos.ZERO), SqlParserPos.ZERO);
            SqlNode col = SqlDdlNodes.column(SqlParserPos.ZERO, colName, type, null, ColumnStrategy.DEFAULT);

            createTblNode.columnList().add(0, col);
        }

        if (pkConstraints.isEmpty()) {
            throw new SqlException(STMT_VALIDATION_ERR, "Table without PRIMARY KEY is not supported");
        }

        if (pkConstraints.size() > 1) {
            throw new SqlException(STMT_VALIDATION_ERR, "Unexpected number of primary key constraints ["
                    + "expected at most one, but was " + pkConstraints.size() + "; "
                    + "querySql=\"" + ctx.query() + "\"]");
        }

        IgniteSqlPrimaryKeyConstraint pkConstraint = pkConstraints.get(0);
        SqlNodeList columnNodes = pkConstraint.getColumnList();

        List<String> pkColumns = new ArrayList<>(columnNodes.size());
        List<CatalogColumnCollation> pkCollations = new ArrayList<>(columnNodes.size());

        IgniteSqlPrimaryKeyIndexType pkIndexType = pkConstraint.getIndexType();
        boolean supportCollation = pkIndexType == IgniteSqlPrimaryKeyIndexType.SORTED;

        parseColumnList(pkConstraint.getColumnList(), pkColumns, pkCollations, supportCollation);

        TablePrimaryKey primaryKey;

        switch (pkIndexType) {
            case SORTED:
                primaryKey = TableSortedPrimaryKey.builder()
                        .columns(pkColumns)
                        .collations(pkCollations)
                        .build();
                break;

            case HASH:
            case IMPLICIT_HASH:
                primaryKey = TableHashPrimaryKey.builder()
                        .columns(pkColumns)
                        .build();
                break;

            default:
                throw new IllegalArgumentException("Unexpected primary key index type: " + pkIndexType);
        }

        List<String> colocationColumns = createTblNode.colocationColumns() == null
                ? null
                : createTblNode.colocationColumns().getList().stream()
                        .map(SqlIdentifier.class::cast)
                        .map(SqlIdentifier::getSimple)
                        .collect(Collectors.toList());

        List<SqlColumnDeclaration> colDeclarations = createTblNode.columnList().getList().stream()
                .filter(SqlColumnDeclaration.class::isInstance)
                .map(SqlColumnDeclaration.class::cast)
                .collect(Collectors.toList());

        List<ColumnParams> columns = new ArrayList<>(colDeclarations.size());

        for (SqlColumnDeclaration col : colDeclarations) {
            if (!col.name.isSimple()) {
                throw new SqlException(STMT_VALIDATION_ERR, "Unexpected value of columnName ["
                        + "expected a simple identifier, but was " + col.name + "; "
                        + "querySql=\"" + ctx.query() + "\"]");
            }

            columns.add(convertColumnDeclaration(col, ctx.planner(), !pkColumns.contains(col.name.getSimple())));
        }

        return tblBuilder.schemaName(deriveSchemaName(createTblNode.name(), ctx))
                .tableName(deriveObjectName(createTblNode.name(), ctx, "tableName"))
                .columns(columns)
                .primaryKey(primaryKey)
                .colocationColumns(colocationColumns)
                .ifTableExists(createTblNode.ifNotExists())
                .build();
    }

    private static ColumnParams convertColumnDeclaration(SqlColumnDeclaration col, IgnitePlanner planner, boolean nullable) {
        assert col.name.isSimple();

        String name = col.name.getSimple();
        RelDataType relType = planner.convert(col.dataType, nullable);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-15200
        //  Remove this after interval type support is added.
        if (SqlTypeUtil.isInterval(relType)) {
            String error = format(
                    "Type {} cannot be used in a column definition [column={}].", 
                    relType.getSqlTypeName().getSpaceName(),
                    name
            );
            throw new SqlException(STMT_VALIDATION_ERR, error);
        }

        ColumnTypeParams typeParams = new ColumnTypeParams(relType);

        return ColumnParams.builder()
                .name(name)
                .type(typeParams.colType)
                .nullable(relType.isNullable())
                .precision(typeParams.precision)
                .scale(typeParams.scale)
                .length(typeParams.length)
                .defaultValue(convertDefault(col.expression, relType, name))
                .build();
    }

    private static DefaultValue convertDefault(@Nullable SqlNode expression, RelDataType relType, String name) {
        if (expression == null) {
            return DefaultValue.constant(null);
        }

        if (expression instanceof SqlIdentifier) {
            return DefaultValue.functionCall(((SqlIdentifier) expression).getSimple());
        }

        if (expression instanceof SqlLiteral) {
            ColumnType columnType = columnType(relType);

            Object val = fromLiteral(columnType, name, (SqlLiteral) expression, relType.getPrecision(), relType.getScale());
            return DefaultValue.constant(val);
        }

        throw new IllegalArgumentException("Unsupported default expression: " + expression.getKind());
    }

    /**
     * Converts the given `{@code ALTER TABLE ... ADD COLUMN}` AST into the {@link IgniteSqlAlterTableAddColumn} catalog command.
     */
    private CatalogCommand convertAlterTableAdd(IgniteSqlAlterTableAddColumn alterTblNode, PlanningContext ctx) {
        AlterTableAddColumnCommandBuilder builder = AlterTableAddColumnCommand.builder();

        builder.schemaName(deriveSchemaName(alterTblNode.name(), ctx));
        builder.tableName(deriveObjectName(alterTblNode.name(), ctx, "table name"));
        builder.ifTableExists(alterTblNode.ifExists());

        List<ColumnParams> columns = new ArrayList<>(alterTblNode.columns().size());

        for (SqlNode colNode : alterTblNode.columns()) {
            assert colNode instanceof SqlColumnDeclaration : colNode.getClass();
            SqlColumnDeclaration col = (SqlColumnDeclaration) colNode;
            Boolean nullable = col.dataType.getNullable();

            String colName = col.name.getSimple();
            if (IgniteSqlValidator.isSystemFieldName(colName)) {
                throw new SqlException(STMT_VALIDATION_ERR, "Failed to validate query. "
                        + "Column '" + colName + "' is reserved name.");
            }

            columns.add(convertColumnDeclaration(col, ctx.planner(), nullable != null ? nullable : true));
        }

        builder.columns(columns);

        return builder.build();
    }

    /**
     * Converts the given `{@code ALTER TABLE ... ALTER COLUMN}` AST into the {@link AlterTableAlterColumnCommand} catalog command.
     */
    private CatalogCommand convertAlterColumn(IgniteSqlAlterColumn alterColumnNode, PlanningContext ctx) {
        AlterTableAlterColumnCommandBuilder builder = AlterTableAlterColumnCommand.builder();

        builder.schemaName(deriveSchemaName(alterColumnNode.name(), ctx));
        builder.tableName(deriveObjectName(alterColumnNode.name(), ctx, "table name"));
        builder.ifTableExists(alterColumnNode.ifExists());
        builder.columnName(alterColumnNode.columnName().getSimple());

        RelDataType relType = null;

        SqlDataTypeSpec colTypeSpec = alterColumnNode.dataType();

        if (colTypeSpec != null) {
            relType = ctx.planner().convert(colTypeSpec, true);

            ColumnTypeParams typeParams = new ColumnTypeParams(relType);

            builder.type(typeParams.colType);

            if (typeParams.length != null) {
                builder.length(typeParams.length);
            } else {
                if (typeParams.precision != null) {
                    builder.precision(typeParams.precision);
                }

                if (typeParams.scale != null) {
                    builder.scale(typeParams.scale);
                }
            }
        }

        Boolean notNull = alterColumnNode.notNull();

        if (notNull != null) {
            builder.nullable(!notNull);
        }

        if (alterColumnNode.expression() != null) {
            SqlNode expr = alterColumnNode.expression();

            DeferredDefaultValue resolveDfltFunc;

            int precision = relType == null ? PRECISION_NOT_SPECIFIED : relType.getPrecision();
            int scale = relType == null ? SCALE_NOT_SPECIFIED : relType.getScale();
            String name = alterColumnNode.columnName().getSimple();

            if (expr instanceof SqlLiteral) {
                resolveDfltFunc = type -> DefaultValue.constant(fromLiteral(type, name, (SqlLiteral) expr, precision, scale));
            } else {
                throw new IllegalStateException("Invalid expression type " + expr.getKind());
            }

            builder.deferredDefaultValue(resolveDfltFunc);
        }

        return builder.build();
    }

    /**
     * Converts the given '{@code ALTER TABLE ... DROP COLUMN}' AST to the {@link AlterTableDropColumnCommand} catalog command.
     */
    private CatalogCommand convertAlterTableDrop(IgniteSqlAlterTableDropColumn alterTblNode, PlanningContext ctx) {
        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder();

        builder.schemaName(deriveSchemaName(alterTblNode.name(), ctx));
        builder.tableName(deriveObjectName(alterTblNode.name(), ctx, "table name"));
        builder.ifTableExists(alterTblNode.ifExists());

        Set<String> cols = new HashSet<>(alterTblNode.columns().size());
        alterTblNode.columns().forEach(c -> cols.add(((SqlIdentifier) c).getSimple()));

        builder.columns(cols);

        return builder.build();
    }

    /**
     * Converts the given '{@code DROP TABLE}' AST to the {@link DropTableCommand} catalog command.
     */
    private CatalogCommand convertDropTable(IgniteSqlDropTable dropTblNode, PlanningContext ctx) {
        DropTableCommandBuilder builder = DropTableCommand.builder();

        return builder.schemaName(deriveSchemaName(dropTblNode.name(), ctx))
                .tableName(deriveObjectName(dropTblNode.name(), ctx, "tableName"))
                .ifTableExists(dropTblNode.ifExists)
                .build();
    }

    /**
     * Converts '{@code CREATE INDEX}' AST to the appropriate catalog command.
     */
    private CatalogCommand convertAddIndex(IgniteSqlCreateIndex sqlCmd, PlanningContext ctx) {
        boolean sortedIndex = sqlCmd.type() == IgniteSqlIndexType.SORTED || sqlCmd.type() == IgniteSqlIndexType.IMPLICIT_SORTED;
        SqlNodeList columnList = sqlCmd.columnList();
        List<String> columns = new ArrayList<>(columnList.size());
        List<CatalogColumnCollation> collations = new ArrayList<>(columnList.size());

        parseColumnList(columnList, columns, collations, sortedIndex);

        if (sortedIndex) {
            return CreateSortedIndexCommand.builder()
                    .schemaName(deriveSchemaName(sqlCmd.tableName(), ctx))
                    .tableName(deriveObjectName(sqlCmd.tableName(), ctx, "table name"))
                    .ifNotExists(sqlCmd.ifNotExists())
                    .indexName(sqlCmd.indexName().getSimple())
                    .columns(columns)
                    .collations(collations)
                    .build();
        } else {
            return CreateHashIndexCommand.builder()
                    .schemaName(deriveSchemaName(sqlCmd.tableName(), ctx))
                    .tableName(deriveObjectName(sqlCmd.tableName(), ctx, "table name"))
                    .ifNotExists(sqlCmd.ifNotExists())
                    .indexName(sqlCmd.indexName().getSimple())
                    .columns(columns)
                    .build();
        }
    }

    private static void parseColumnList(
            SqlNodeList columnList,
            List<String> columns,
            List<CatalogColumnCollation> collations,
            boolean supportCollation
    ) {
        for (SqlNode col : columnList.getList()) {
            boolean asc = true;

            if (col.getKind() == SqlKind.DESCENDING) {
                col = ((SqlCall) col).getOperandList().get(0);

                asc = false;
            }

            String columnName = ((SqlIdentifier) col).getSimple();
            columns.add(columnName);
            if (supportCollation) {
                collations.add(CatalogColumnCollation.get(asc, !asc));
            }
        }
    }

    /**
     * Converts '{@code DROP INDEX}' AST to the appropriate catalog command.
     */
    private CatalogCommand convertDropIndex(IgniteSqlDropIndex sqlCmd, PlanningContext ctx) {
        String schemaName = deriveSchemaName(sqlCmd.indexName(), ctx);
        String indexName = deriveObjectName(sqlCmd.indexName(), ctx, "index name");

        return DropIndexCommand.builder()
                .schemaName(schemaName)
                .indexName(indexName)
                .ifExists(sqlCmd.ifExists())
                .build();
    }

    /**
     * Converts the given '{@code CREATE ZONE}' AST to the {@link CreateZoneCommand} catalog command.
     */
    private CatalogCommand convertCreateZone(IgniteSqlCreateZone createZoneNode, PlanningContext ctx) {
        CreateZoneCommandBuilder builder = CreateZoneCommand.builder();

        builder.zoneName(deriveObjectName(createZoneNode.name(), ctx, "zoneName"));
        builder.ifNotExists(createZoneNode.ifNotExists());

        if (createZoneNode.createOptionList() == null) {
            throw new SqlException(STMT_VALIDATION_ERR, STORAGE_PROFILES + " option cannot be null");
        }

        Set<String> remainingKnownOptions = new HashSet<>(knownZoneOptionNames);

        for (SqlNode optionNode : createZoneNode.createOptionList().getList()) {
            IgniteSqlZoneOption option = (IgniteSqlZoneOption) optionNode;

            assert option.key().isSimple() : option.key();

            String optionName = option.key().getSimple().toUpperCase();

            DdlOptionInfo<CreateZoneCommandBuilder, ?> zoneOptionInfo = null;

            if (remainingKnownOptions.remove(optionName)) {
                zoneOptionInfo = zoneOptionInfos.get(ZoneOptionEnum.valueOf(optionName));
            } else if (knownZoneOptionNames.contains(optionName)) {
                throw duplicateZoneOption(ctx, optionName);
            }

            if (zoneOptionInfo == null) {
                throw unexpectedZoneOption(ctx, optionName);
            }

            updateCommandOption("Zone", optionName, option.value(), zoneOptionInfo, ctx.query(), builder);
        }

        if (remainingKnownOptions.contains(STORAGE_PROFILES.name())) {
            throw new SqlException(STMT_VALIDATION_ERR, STORAGE_PROFILES + " option cannot be null");
        }

        return builder.build();
    }


    /**
     * Converts the given '{@code ALTER ZONE}' AST to the {@link AlterZoneCommand} catalog command.
     */
    private CatalogCommand convertAlterZoneSet(IgniteSqlAlterZoneSet alterZoneSet, PlanningContext ctx) {
        AlterZoneCommandBuilder builder = AlterZoneCommand.builder();

        builder.zoneName(deriveObjectName(alterZoneSet.name(), ctx, "zoneName"));
        builder.ifExists(alterZoneSet.ifExists());

        Set<String> remainingKnownOptions = new HashSet<>(knownZoneOptionNames);

        for (SqlNode optionNode : alterZoneSet.alterOptionsList().getList()) {
            IgniteSqlZoneOption option = (IgniteSqlZoneOption) optionNode;
            String optionName = option.key().getSimple().toUpperCase();

            if (!knownZoneOptionNames.contains(optionName)) {
                throw unexpectedZoneOption(ctx, optionName);
            } else if (!remainingKnownOptions.remove(optionName)) {
                throw duplicateZoneOption(ctx, optionName);
            }

            DdlOptionInfo<AlterZoneCommandBuilder, ?> zoneOptionInfo = alterZoneOptionInfos.get(ZoneOptionEnum.valueOf(optionName));

            assert zoneOptionInfo != null : optionName;
            assert option.value() instanceof SqlLiteral : option.value();

            updateCommandOption("Zone", optionName, option.value(), zoneOptionInfo, ctx.query(), builder);
        }

        return builder.build();
    }

    /**
     * Converts the given '{@code ALTER ZONE ... SET DEFAULT}' AST node to the {@link AlterZoneSetDefaultCommand} catalog command.
     */
    private CatalogCommand convertAlterZoneSetDefault(IgniteSqlAlterZoneSetDefault alterZoneSetDefault, PlanningContext ctx) {
        return AlterZoneSetDefaultCommand.builder()
                .zoneName(deriveObjectName(alterZoneSetDefault.name(), ctx, "zoneName"))
                .ifExists(alterZoneSetDefault.ifExists())
                .build();
    }

    /**
     * Converts the given '{@code ALTER ZONE ... RENAME TO}' AST node to the {@link RenameZoneCommand} catalog command.
     */
    private CatalogCommand convertAlterZoneRename(IgniteSqlAlterZoneRenameTo alterZoneRename, PlanningContext ctx) {
        return RenameZoneCommand.builder()
                .zoneName(deriveObjectName(alterZoneRename.name(), ctx, "zoneName"))
                .newZoneName(alterZoneRename.newName().getSimple())
                .ifExists(alterZoneRename.ifExists())
                .build();
    }

    /**
     * Converts the given '{@code DROP ZONE}' AST to the {@link DropZoneCommand} catalog command.
     */
    private CatalogCommand convertDropZone(IgniteSqlDropZone dropZoneNode, PlanningContext ctx) {
        return DropZoneCommand.builder()
                .zoneName(deriveObjectName(dropZoneNode.name(), ctx, "zoneName"))
                .ifExists(dropZoneNode.ifExists())
                .build();
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

        if (ctx.catalogReader().getRootSchema().getSubSchema(schemaName, true) == null) {
            throw new SchemaNotFoundException(schemaName);
        }

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
                String msg = format(
                        "Invalid {} value kind [kind={}, expectedKind=(IDENTIFIER, LITERAL), query={}]",
                        sqlObjName.toLowerCase(), valueKind, query
                );
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
            String msg = format(
                    "{} option validation failed [option={}, err={}, query={}]",
                    sqlObjName, optId, e.getMessage(), query
            );
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
            String msg = format(
                    "Invalid {} option type [option={}, expectedType={}, query={}]",
                    sqlObjName.toLowerCase(), optId, optInfo.type.getSimpleName(), query
            );
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
                format("Unexpected zone option [option={}, query={}]", optionName, ctx.query()));
    }

    private static IgniteException duplicateZoneOption(PlanningContext ctx, String optionName) {
        return new SqlException(STMT_VALIDATION_ERR,
                format("Duplicate zone option has been specified [option={}, query={}]", optionName, ctx.query()));
    }

    /** Helper for obtaining scale, precision and length parameters uniformly. */
    static class ColumnTypeParams {
        final ColumnType colType;
        @Nullable Integer precision = null;
        @Nullable Integer scale = null;
        @Nullable Integer length = null;

        ColumnTypeParams(RelDataType relType) {
            colType = columnType(relType);

            if (colType.lengthAllowed()) {
                length = relType.getPrecision() == PRECISION_NOT_SPECIFIED
                        ? defaultLength(colType, DEFAULT_LENGTH)
                        : relType.getPrecision();
            } else {
                if (relType.getSqlTypeName().allowsPrec() && relType.getPrecision() != PRECISION_NOT_SPECIFIED) {
                    precision = relType.getPrecision();
                }

                if (relType.getSqlTypeName().allowsScale() && relType.getScale() != SCALE_NOT_SPECIFIED) {
                    scale = relType.getScale();
                }
            }
        }
    }
}
