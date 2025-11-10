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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_LENGTH;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultLength;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.CONSISTENCY_MODE;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_FILTER;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DISTRIBUTION_ALGORITHM;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.PARTITIONS;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.QUORUM_SIZE;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.REPLICAS;
import static org.apache.ignite.internal.sql.engine.sql.IgniteSqlTablePropertyKey.MIN_STALE_ROWS_COUNT;
import static org.apache.ignite.internal.sql.engine.sql.IgniteSqlTablePropertyKey.STALE_ROWS_FRACTION;
import static org.apache.ignite.internal.sql.engine.util.IgniteMath.convertToByteExact;
import static org.apache.ignite.internal.sql.engine.util.IgniteMath.convertToIntExact;
import static org.apache.ignite.internal.sql.engine.util.IgniteMath.convertToShortExact;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.columnType;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.fromInternal;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
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
import org.apache.ignite.internal.catalog.commands.AlterTableSetPropertyCommand;
import org.apache.ignite.internal.catalog.commands.AlterTableSetPropertyCommandBuilder;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommand;
import org.apache.ignite.internal.catalog.commands.AlterZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.AlterZoneSetDefaultCommand;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateSchemaCommand;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.apache.ignite.internal.catalog.commands.CreateTableCommandBuilder;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommandBuilder;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DeferredDefaultValue;
import org.apache.ignite.internal.catalog.commands.DropIndexCommand;
import org.apache.ignite.internal.catalog.commands.DropSchemaCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.commands.DropZoneCommand;
import org.apache.ignite.internal.catalog.commands.RenameZoneCommand;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.commands.TableHashPrimaryKey;
import org.apache.ignite.internal.catalog.commands.TablePrimaryKey;
import org.apache.ignite.internal.catalog.commands.TableSortedPrimaryKey;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.partitiondistribution.DistributionAlgorithm;
import org.apache.ignite.internal.sql.engine.exec.exp.IgniteSqlFunctions;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.IgniteSqlValidator;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterTableAddColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterTableDropColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterTableSetProperties;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterZoneRenameTo;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterZoneSet;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterZoneSetDefault;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateIndex;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateSchema;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateTable;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateZone;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropIndex;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropSchema;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropSchemaBehavior;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropTable;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropZone;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlIndexType;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlPrimaryKeyConstraint;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlPrimaryKeyIndexType;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlTableProperty;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlTablePropertyKey;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlZoneOption;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlZoneOptionMode;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.IgniteSqlDateTimeUtils;
import org.apache.ignite.internal.table.distributed.TableStatsStalenessConfiguration;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Converts the DDL AST tree to the appropriate catalog command.
 */
public class DdlSqlToCommandConverter {
    /** Mapping: Zone option ID -> DDL option info. */
    private final Map<ZoneOptionEnum, DdlOptionInfo<CreateZoneCommandBuilder, ?>> zoneOptionInfos;

    /** Mapping: Zone option ID -> DDL option info. */
    private final Map<ZoneOptionEnum, DdlOptionInfo<AlterZoneCommandBuilder, ?>> alterZoneOptionInfos;

    private final Map<IgniteSqlTablePropertyKey, DdlOptionInfo<CreateTableCommandBuilder, ?>> createTablePropertiesInfos;
    private final Map<IgniteSqlTablePropertyKey, DdlOptionInfo<AlterTableSetPropertyCommandBuilder, ?>> alterTablePropertiesInfos;

    /** DDL option info for an integer CREATE ZONE REPLICAS option. */
    private final DdlOptionInfo<CreateZoneCommandBuilder, Integer> createReplicasOptionInfo;

    /** DDL option info for an integer ALTER ZONE REPLICAS option. */
    private final DdlOptionInfo<AlterZoneCommandBuilder, Integer> alterReplicasOptionInfo;

    /** Zone options set. */
    private final Set<String> knownZoneOptionNames;

    /** Storage profiles validator. */
    private final StorageProfileValidator storageProfileValidator;

    /** Node filter validator. */
    private final NodeFilterValidator nodeFilterValidator;

    private final Supplier<TableStatsStalenessConfiguration> stalenessProperties;

    /**
     * Constructor.
     *
     * @param storageProfileValidator Storage profile names validator.
     * @param nodeFilterValidator Node filter validator.
     * @param stalenessProperties Data staleness properties.
     */
    public DdlSqlToCommandConverter(
            StorageProfileValidator storageProfileValidator,
            NodeFilterValidator nodeFilterValidator,
            Supplier<TableStatsStalenessConfiguration> stalenessProperties
    ) {
        knownZoneOptionNames = EnumSet.allOf(ZoneOptionEnum.class)
                .stream()
                .map(Enum::name)
                .collect(Collectors.toSet());

        // CREATE ZONE options.
        zoneOptionInfos = new EnumMap<>(Map.of(
                PARTITIONS, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommandBuilder::partitions),
                // We can't properly validate quorum size since it depends on the replicas number.
                QUORUM_SIZE, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommandBuilder::quorumSize),
                // TODO https://issues.apache.org/jira/browse/IGNITE-22162 appropriate setter method should be used.
                DISTRIBUTION_ALGORITHM, new DdlOptionInfo<>(String.class, null, (builder, params) -> {}),
                DATA_NODES_FILTER, new DdlOptionInfo<>(String.class, null, CreateZoneCommandBuilder::filter),
                DATA_NODES_AUTO_ADJUST_SCALE_UP,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommandBuilder::dataNodesAutoAdjustScaleUp),
                DATA_NODES_AUTO_ADJUST_SCALE_DOWN,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommandBuilder::dataNodesAutoAdjustScaleDown),
                CONSISTENCY_MODE, new DdlOptionInfo<>(String.class, this::checkEmptyString,
                        (builder, params) -> builder.consistencyModeParams(parseConsistencyMode(params)))
        ));

        createReplicasOptionInfo = new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, CreateZoneCommandBuilder::replicas);

        // ALTER ZONE options.
        alterZoneOptionInfos = new EnumMap<>(Map.of(
                PARTITIONS, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneCommandBuilder::partitions),
                // We can't properly validate quorum size since it depends on the replicas number.
                QUORUM_SIZE, new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneCommandBuilder::quorumSize),
                DATA_NODES_FILTER, new DdlOptionInfo<>(String.class, null, AlterZoneCommandBuilder::filter),
                DATA_NODES_AUTO_ADJUST_SCALE_UP,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneCommandBuilder::dataNodesAutoAdjustScaleUp),
                DATA_NODES_AUTO_ADJUST_SCALE_DOWN,
                new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneCommandBuilder::dataNodesAutoAdjustScaleDown)
        ));

        alterReplicasOptionInfo = new DdlOptionInfo<>(Integer.class, this::checkPositiveNumber, AlterZoneCommandBuilder::replicas);

        createTablePropertiesInfos = new EnumMap<>(Map.of(
                MIN_STALE_ROWS_COUNT, new DdlOptionInfo<>(Long.class, null, CreateTableCommandBuilder::minStaleRowsCount),
                STALE_ROWS_FRACTION, new DdlOptionInfo<>(Double.class, null, CreateTableCommandBuilder::staleRowsFraction)
        ));

        alterTablePropertiesInfos = new EnumMap<>(Map.of(
                MIN_STALE_ROWS_COUNT, new DdlOptionInfo<>(Long.class, null, AlterTableSetPropertyCommandBuilder::minStaleRowsCount),
                STALE_ROWS_FRACTION, new DdlOptionInfo<>(Double.class, null, AlterTableSetPropertyCommandBuilder::staleRowsFraction)
        ));

        this.storageProfileValidator = storageProfileValidator;
        this.nodeFilterValidator = nodeFilterValidator;

        this.stalenessProperties = stalenessProperties;
    }

    /**
     * Parse string representation of consistency mode.
     *
     * @param consistencyMode String representation of consistency mode.
     * @return Consistency mode
     */
    private static ConsistencyMode parseConsistencyMode(String consistencyMode) {
        try {
            return ConsistencyMode.valueOf(consistencyMode);
        } catch (IllegalArgumentException ignored) {
            throw new SqlException(STMT_VALIDATION_ERR, "Failed to parse consistency mode: " + consistencyMode
                    + ". Valid values are: " + Arrays.toString(ConsistencyMode.values()));
        }
    }

    /**
     * Converts a given ddl AST to a catalog command.
     *
     * @param ddlNode Root node of the given AST.
     * @param ctx Planning context.
     * @return Catalog command.
     */
    public CompletableFuture<CatalogCommand> convert(SqlDdl ddlNode, PlanningContext ctx) {
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

        if (ddlNode instanceof IgniteSqlAlterTableSetProperties) {
            return convertAlterTableSet((IgniteSqlAlterTableSetProperties) ddlNode, ctx);
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

        if (ddlNode instanceof IgniteSqlCreateSchema) {
            return convertCreateSchema((IgniteSqlCreateSchema) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlDropSchema) {
            return convertDropSchema((IgniteSqlDropSchema) ddlNode, ctx);
        }

        throw new SqlException(STMT_VALIDATION_ERR, "Unsupported operation ["
                + "sqlNodeKind=" + ddlNode.getKind() + "; "
                + "querySql=\"" + ctx.query() + "\"]");
    }

    private CompletableFuture<CatalogCommand> convertAlterTableSet(IgniteSqlAlterTableSetProperties alterTableSet, PlanningContext ctx) {
        AlterTableSetPropertyCommandBuilder builder = AlterTableSetPropertyCommand.builder();

        builder.schemaName(deriveSchemaName(alterTableSet.name(), ctx));
        builder.tableName(deriveObjectName(alterTableSet.name(), ctx, "tableName"));
        builder.ifTableExists(alterTableSet.ifExists());

        handleTablePropertyList(alterTableSet.propertyList(), alterTablePropertiesInfos, ctx, builder);

        return completedFuture(builder.build());
    }

    private CompletableFuture<CatalogCommand> convertCreateSchema(IgniteSqlCreateSchema ddlNode, PlanningContext ctx) {
        return completedFuture(CreateSchemaCommand.builder()
                .name(deriveObjectName(ddlNode.name(), ctx, "schemaName"))
                .ifNotExists(ddlNode.ifNotExists())
                .build());
    }

    private CompletableFuture<CatalogCommand> convertDropSchema(IgniteSqlDropSchema ddlNode, PlanningContext ctx) {
        return completedFuture(DropSchemaCommand.builder()
                .name(deriveObjectName(ddlNode.name(), ctx, "schemaName"))
                .ifExists(ddlNode.ifExists())
                .cascade(ddlNode.behavior() == IgniteSqlDropSchemaBehavior.CASCADE)
                .build());
    }

    /**
     * Converts the given '{@code CREATE TABLE}' AST to the {@link CreateTableCommand} catalog command.
     */
    private CompletableFuture<CatalogCommand> convertCreateTable(IgniteSqlCreateTable createTblNode, PlanningContext ctx) {
        CreateTableCommandBuilder tblBuilder = CreateTableCommand.builder();

        List<IgniteSqlPrimaryKeyConstraint> pkConstraints = createTblNode.columnList().getList().stream()
                .filter(IgniteSqlPrimaryKeyConstraint.class::isInstance)
                .map(IgniteSqlPrimaryKeyConstraint.class::cast)
                .collect(toList());

        for (SqlNode sqlNode : createTblNode.columnList().getList()) {
            if (sqlNode instanceof SqlColumnDeclaration) {
                String colName = ((SqlColumnDeclaration) sqlNode).name.getSimple();

                if (IgniteSqlValidator.isSystemColumnName(colName)) {
                    throw new SqlException(STMT_VALIDATION_ERR, "Failed to validate query. "
                            + "Column '" + colName + "' is reserved name.");
                }
            }
        }

        if (pkConstraints.isEmpty() && Commons.implicitPkEnabled()) {
            SqlIdentifier colName = new SqlIdentifier(Commons.IMPLICIT_PK_COL_NAME, SqlParserPos.ZERO);

            pkConstraints.add(new IgniteSqlPrimaryKeyConstraint(SqlParserPos.ZERO, null, SqlNodeList.of(colName),
                    IgniteSqlPrimaryKeyIndexType.IMPLICIT_HASH));

            SqlDataTypeSpec type = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.UUID, SqlParserPos.ZERO), SqlParserPos.ZERO);
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
        String pkName = null;
        SqlIdentifier pkIdentifier = pkConstraint.getName();
        if (pkIdentifier != null) {
            pkName = deriveObjectName(pkIdentifier, ctx, "name of pk constraint");
        }
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
                        .name(pkName)
                        .columns(pkColumns)
                        .collations(pkCollations)
                        .build();
                break;

            case HASH:
            case IMPLICIT_HASH:
                primaryKey = TableHashPrimaryKey.builder()
                        .name(pkName)
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
                        .collect(toList());

        List<SqlColumnDeclaration> colDeclarations = createTblNode.columnList().getList().stream()
                .filter(SqlColumnDeclaration.class::isInstance)
                .map(SqlColumnDeclaration.class::cast)
                .collect(toList());

        List<ColumnParams> columns = new ArrayList<>(colDeclarations.size());

        for (SqlColumnDeclaration col : colDeclarations) {
            if (!col.name.isSimple()) {
                throw new SqlException(STMT_VALIDATION_ERR, "Unexpected value of columnName ["
                        + "expected a simple identifier, but was " + col.name + "; "
                        + "querySql=\"" + ctx.query() + "\"]");
            }

            columns.add(convertColumnDeclaration(col, ctx.planner(), !pkColumns.contains(col.name.getSimple())));
        }

        String storageProfile = null;
        if (createTblNode.storageProfile() != null) {
            assert createTblNode.storageProfile().getKind() == SqlKind.LITERAL;

            storageProfile = ((SqlLiteral) createTblNode.storageProfile()).getValueAs(String.class);
            checkEmptyString(storageProfile);
        }

        String zone = createTblNode.zone() == null ? null : createTblNode.zone().getSimple();

        TableStatsStalenessConfiguration properties = stalenessProperties.get();
        tblBuilder
                .staleRowsFraction(properties.staleRowsFraction())
                .minStaleRowsCount(properties.minStaleRowsCount());

        SqlNodeList propertyList = createTblNode.tableProperties();
        if (propertyList != null) {
            handleTablePropertyList(propertyList, createTablePropertiesInfos, ctx, tblBuilder);
        }

        CatalogCommand command = tblBuilder.schemaName(deriveSchemaName(createTblNode.name(), ctx))
                .tableName(deriveObjectName(createTblNode.name(), ctx, "tableName"))
                .columns(columns)
                .primaryKey(primaryKey)
                .colocationColumns(colocationColumns)
                .zone(zone)
                .storageProfile(storageProfile)
                .ifTableExists(createTblNode.ifNotExists())
                .build();

        return completedFuture(command);
    }

    private static ColumnParams convertColumnDeclaration(SqlColumnDeclaration col, IgnitePlanner planner, boolean nullable) {
        assert col.name.isSimple();

        String name = col.name.getSimple();

        RelDataType relType;
        try {
            relType = planner.convert(col.dataType, nullable);
        } catch (CalciteContextException e) {
            String errorMessage = e.getMessage();
            if (errorMessage == null) {
                errorMessage = "Unable to resolve data type";
            }

            String message = format("{} [column={}]", errorMessage, name);
            throw new SqlException(STMT_VALIDATION_ERR, message, e);
        }

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17373
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

        DeferredDefaultValue deferredDefaultValue = convertDefaultExpression(expression, name, relType);
        ColumnType columnType = columnType(relType);
        return deferredDefaultValue.derive(columnType);
    }

    /**
     * Converts the given `{@code ALTER TABLE ... ADD COLUMN}` AST into the {@link IgniteSqlAlterTableAddColumn} catalog command.
     */
    private CompletableFuture<CatalogCommand> convertAlterTableAdd(IgniteSqlAlterTableAddColumn alterTblNode, PlanningContext ctx) {
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
            if (IgniteSqlValidator.isSystemColumnName(colName)) {
                throw new SqlException(STMT_VALIDATION_ERR, "Failed to validate query. "
                        + "Column '" + colName + "' is reserved name.");
            }

            columns.add(convertColumnDeclaration(col, ctx.planner(), nullable != null ? nullable : true));
        }

        builder.columns(columns);

        return completedFuture(builder.build());
    }

    /**
     * Converts the given `{@code ALTER TABLE ... ALTER COLUMN}` AST into the {@link AlterTableAlterColumnCommand} catalog command.
     */
    private CompletableFuture<CatalogCommand> convertAlterColumn(IgniteSqlAlterColumn alterColumnNode, PlanningContext ctx) {
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

        SqlNode defaultExpr = alterColumnNode.expression();
        if (defaultExpr != null) {
            String columnName = alterColumnNode.name().getSimple();
            DeferredDefaultValue deferredDfltFunc = convertDefaultExpression(defaultExpr, columnName, relType);

            builder.deferredDefaultValue(deferredDfltFunc);
        }

        return completedFuture(builder.build());
    }

    private static DeferredDefaultValue convertDefaultExpression(
            SqlNode expr,
            String name,
            @Nullable RelDataType relType
    ) {
        if (expr instanceof SqlLiteral) {
            int precision = relType == null ? PRECISION_NOT_SPECIFIED : relType.getPrecision();
            int scale = relType == null ? SCALE_NOT_SPECIFIED : relType.getScale();

            return type -> DefaultValue.constant(fromLiteral(type, name, (SqlLiteral) expr, precision, scale));
        } else if (expr instanceof SqlIdentifier && ((SqlIdentifier) expr).isSimple()) {

            return type -> DefaultValue.functionCall(((SqlIdentifier) expr).getSimple());
        } else if (expr instanceof SqlCall && ((SqlCall) expr).getOperandList().isEmpty()) {
            SqlCall call = (SqlCall) expr;
            String functionName = call.getOperator().getName();

            return type -> DefaultValue.functionCall(functionName);
        }

        // Report compound ids, expressions, and non-zero argument function calls as their SQL string representation.
        throw new SqlException(STMT_VALIDATION_ERR, "Unsupported default expression: " + expr);
    }

    /**
     * Converts the given '{@code ALTER TABLE ... DROP COLUMN}' AST to the {@link AlterTableDropColumnCommand} catalog command.
     */
    private CompletableFuture<CatalogCommand> convertAlterTableDrop(IgniteSqlAlterTableDropColumn alterTblNode, PlanningContext ctx) {
        AlterTableDropColumnCommandBuilder builder = AlterTableDropColumnCommand.builder();

        builder.schemaName(deriveSchemaName(alterTblNode.name(), ctx));
        builder.tableName(deriveObjectName(alterTblNode.name(), ctx, "table name"));
        builder.ifTableExists(alterTblNode.ifExists());

        Set<String> cols = new HashSet<>(alterTblNode.columns().size());
        alterTblNode.columns().forEach(c -> cols.add(((SqlIdentifier) c).getSimple()));

        builder.columns(cols);

        return completedFuture(builder.build());
    }

    /**
     * Converts the given '{@code DROP TABLE}' AST to the {@link DropTableCommand} catalog command.
     */
    private CompletableFuture<CatalogCommand> convertDropTable(IgniteSqlDropTable dropTblNode, PlanningContext ctx) {
        return completedFuture(DropTableCommand.builder()
                .schemaName(deriveSchemaName(dropTblNode.name(), ctx))
                .tableName(deriveObjectName(dropTblNode.name(), ctx, "tableName"))
                .ifTableExists(dropTblNode.ifExists)
                .build());
    }

    /**
     * Converts '{@code CREATE INDEX}' AST to the appropriate catalog command.
     */
    private CompletableFuture<CatalogCommand> convertAddIndex(IgniteSqlCreateIndex sqlCmd, PlanningContext ctx) {
        boolean sortedIndex = sqlCmd.type() == IgniteSqlIndexType.SORTED || sqlCmd.type() == IgniteSqlIndexType.IMPLICIT_SORTED;
        SqlNodeList columnList = sqlCmd.columnList();
        List<String> columns = new ArrayList<>(columnList.size());
        List<CatalogColumnCollation> collations = new ArrayList<>(columnList.size());

        parseColumnList(columnList, columns, collations, sortedIndex);

        CatalogCommand command;

        if (sortedIndex) {
            command = CreateSortedIndexCommand.builder()
                    .schemaName(deriveSchemaName(sqlCmd.tableName(), ctx))
                    .tableName(deriveObjectName(sqlCmd.tableName(), ctx, "table name"))
                    .ifNotExists(sqlCmd.ifNotExists())
                    .indexName(sqlCmd.indexName().getSimple())
                    .columns(columns)
                    .collations(collations)
                    .build();
        } else {
            command = CreateHashIndexCommand.builder()
                    .schemaName(deriveSchemaName(sqlCmd.tableName(), ctx))
                    .tableName(deriveObjectName(sqlCmd.tableName(), ctx, "table name"))
                    .ifNotExists(sqlCmd.ifNotExists())
                    .indexName(sqlCmd.indexName().getSimple())
                    .columns(columns)
                    .build();
        }

        return completedFuture(command);
    }

    private static void parseColumnList(
            SqlNodeList columnList,
            List<String> columns,
            List<CatalogColumnCollation> collations,
            boolean supportCollation
    ) {
        for (SqlNode col : columnList.getList()) {
            boolean asc = true;
            Boolean nullsFirst = null;

            if (col.getKind() == SqlKind.NULLS_FIRST) {
                col = ((SqlCall) col).getOperandList().get(0);

                nullsFirst = true;
            } else if (col.getKind() == SqlKind.NULLS_LAST) {
                col = ((SqlCall) col).getOperandList().get(0);

                nullsFirst = false;
            }

            if (col.getKind() == SqlKind.DESCENDING) {
                col = ((SqlCall) col).getOperandList().get(0);

                asc = false;
            }

            if (nullsFirst == null) {
                nullsFirst = !asc;
            }

            String columnName = ((SqlIdentifier) col).getSimple();
            columns.add(columnName);
            if (supportCollation) {
                collations.add(CatalogColumnCollation.get(asc, nullsFirst));
            }
        }
    }

    /**
     * Converts '{@code DROP INDEX}' AST to the appropriate catalog command.
     */
    private CompletableFuture<CatalogCommand> convertDropIndex(IgniteSqlDropIndex sqlCmd, PlanningContext ctx) {
        String schemaName = deriveSchemaName(sqlCmd.indexName(), ctx);
        String indexName = deriveObjectName(sqlCmd.indexName(), ctx, "index name");

        CatalogCommand command = DropIndexCommand.builder()
                .schemaName(schemaName)
                .indexName(indexName)
                .ifExists(sqlCmd.ifExists())
                .build();

        return completedFuture(command);
    }

    /**
     * Converts the given '{@code CREATE ZONE}' AST to the {@link CreateZoneCommand} catalog command.
     */
    private CompletableFuture<CatalogCommand> convertCreateZone(IgniteSqlCreateZone createZoneNode, PlanningContext ctx) {
        if (createZoneNode.storageProfiles().isEmpty()) {
            throw new SqlException(STMT_VALIDATION_ERR, "STORAGE PROFILES can not be empty");
        }

        CreateZoneCommandBuilder builder = CreateZoneCommand.builder();

        builder.zoneName(deriveObjectName(createZoneNode.name(), ctx, "zoneName"));
        builder.ifNotExists(createZoneNode.ifNotExists());

        Set<String> remainingKnownOptions = new HashSet<>(knownZoneOptionNames);

        for (SqlNode optionNode : createZoneNode.createOptionList()) {
            IgniteSqlZoneOption option = (IgniteSqlZoneOption) optionNode;

            updateZoneOption(option, remainingKnownOptions, zoneOptionInfos, createReplicasOptionInfo, ctx, builder);
        }

        List<StorageProfileParams> profiles = extractProfiles(createZoneNode.storageProfiles());

        Set<String> storageProfileNames = new HashSet<>(profiles.size());

        for (StorageProfileParams profile : profiles) {
            storageProfileNames.add(profile.storageProfile());
        }

        List<IgniteSqlZoneOption> options = createZoneNode.createOptionList().stream()
                .map(IgniteSqlZoneOption.class::cast)
                .collect(toList());

        String nodesFilter = extractLiteralOptionValueFromZoneOptionList(options, DATA_NODES_FILTER.name());

        return storageProfileValidator.validate(storageProfileNames)
                .thenCompose(unused -> nodeFilterValidator.validate(nodesFilter))
                .thenApply(unused -> {
                    builder.storageProfilesParams(profiles);

                    return builder.build();
                });
    }

    @Nullable
    private String extractLiteralOptionValueFromZoneOptionList(List<IgniteSqlZoneOption> options, String optionName) {
        return options.stream()
                .filter(opt -> opt.key().isSimple() && opt.key().getSimple().equalsIgnoreCase(optionName))
                .map(opt -> {
                    DdlOptionInfo optInfo = zoneOptionInfos.get(ZoneOptionEnum.valueOf(optionName));
                    return (String) ((SqlLiteral) opt.value()).getValueAs(optInfo.type);
                })
                .findFirst()
                .orElse(null);
    }

    /**
     * Converts the given '{@code ALTER ZONE}' AST to the {@link AlterZoneCommand} catalog command.
     */
    private CompletableFuture<CatalogCommand> convertAlterZoneSet(IgniteSqlAlterZoneSet alterZoneSet, PlanningContext ctx) {
        AlterZoneCommandBuilder builder = AlterZoneCommand.builder();

        builder.zoneName(deriveObjectName(alterZoneSet.name(), ctx, "zoneName"));
        builder.ifExists(alterZoneSet.ifExists());

        Set<String> remainingKnownOptions = new HashSet<>(knownZoneOptionNames);

        for (SqlNode optionNode : alterZoneSet.alterOptionsList().getList()) {
            IgniteSqlZoneOption option = (IgniteSqlZoneOption) optionNode;

            updateZoneOption(option, remainingKnownOptions, alterZoneOptionInfos, alterReplicasOptionInfo, ctx, builder);
        }

        List<IgniteSqlZoneOption> options = alterZoneSet.alterOptionsList().stream()
                .map(IgniteSqlZoneOption.class::cast)
                .collect(toList());

        String nodeFilter = extractLiteralOptionValueFromZoneOptionList(options, DATA_NODES_FILTER.name());

        return nodeFilterValidator.validate(nodeFilter)
                .thenApply(unused -> builder.build());
    }

    /**
     * Converts the given '{@code ALTER ZONE ... SET DEFAULT}' AST node to the {@link AlterZoneSetDefaultCommand} catalog command.
     */
    private CompletableFuture<CatalogCommand> convertAlterZoneSetDefault(
            IgniteSqlAlterZoneSetDefault alterZoneSetDefault,
            PlanningContext ctx
    ) {
        return completedFuture(AlterZoneSetDefaultCommand.builder()
                .zoneName(deriveObjectName(alterZoneSetDefault.name(), ctx, "zoneName"))
                .ifExists(alterZoneSetDefault.ifExists())
                .build());
    }

    /**
     * Converts the given '{@code ALTER ZONE ... RENAME TO}' AST node to the {@link RenameZoneCommand} catalog command.
     */
    private CompletableFuture<CatalogCommand> convertAlterZoneRename(IgniteSqlAlterZoneRenameTo alterZoneRename, PlanningContext ctx) {
        return completedFuture(RenameZoneCommand.builder()
                .zoneName(deriveObjectName(alterZoneRename.name(), ctx, "zoneName"))
                .newZoneName(alterZoneRename.newName().getSimple())
                .ifExists(alterZoneRename.ifExists())
                .build());
    }

    /**
     * Converts the given '{@code DROP ZONE}' AST to the {@link DropZoneCommand} catalog command.
     */
    private CompletableFuture<CatalogCommand> convertDropZone(IgniteSqlDropZone dropZoneNode, PlanningContext ctx) {
        return completedFuture(DropZoneCommand.builder()
                .zoneName(deriveObjectName(dropZoneNode.name(), ctx, "zoneName"))
                .ifExists(dropZoneNode.ifExists())
                .build());
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

    private <S> void updateZoneOption(
            IgniteSqlZoneOption option,
            Set<String> remainingKnownOptions,
            Map<ZoneOptionEnum, DdlOptionInfo<S, ?>> optionInfos,
            DdlOptionInfo<S, Integer> replicasOptionInfo,
            PlanningContext ctx,
            S target
    ) {
        assert option.key().isSimple() : option.key();

        String optionName = option.key().getSimple().toUpperCase();

        if (!knownZoneOptionNames.contains(optionName)) {
            throw unexpectedZoneOption(ctx, optionName);
        } else if (!remainingKnownOptions.remove(optionName)) {
            throw duplicateZoneOption(ctx, ZoneOptionEnum.valueOf(optionName).sqlName);
        }

        ZoneOptionEnum zoneOption = ZoneOptionEnum.valueOf(optionName);
        DdlOptionInfo<S, ?> zoneOptionInfo = optionInfos.get(zoneOption);

        // Options infos doesn't contain REPLICAS, it's handled separately
        assert zoneOptionInfo != null || zoneOption == REPLICAS : zoneOption.sqlName;

        assert option.value() instanceof SqlLiteral : option.value();
        SqlLiteral literal = (SqlLiteral) option.value();

        if (zoneOption == DATA_NODES_AUTO_ADJUST_SCALE_UP || zoneOption == DATA_NODES_AUTO_ADJUST_SCALE_DOWN) {
            if (literal.getTypeName() == SqlTypeName.SYMBOL) {
                IgniteSqlZoneOptionMode zoneOptionMode = literal.symbolValue(IgniteSqlZoneOptionMode.class);

                if (zoneOptionMode != IgniteSqlZoneOptionMode.SCALE_OFF) {
                    throw new SqlException(STMT_VALIDATION_ERR, format(
                            "Unexpected value of zone auto adjust scale [expected OFF, was {}; query=\"{}\"",
                            zoneOptionMode, ctx.query()
                    ));
                }

                // Directly set the option value and return
                zoneOptionInfo.setter.accept(target, Commons.cast(INFINITE_TIMER_VALUE));

                return;
            }
        }

        if (zoneOption == REPLICAS) {
            if (literal.getTypeName() == SqlTypeName.SYMBOL) {
                IgniteSqlZoneOptionMode zoneOptionMode = literal.symbolValue(IgniteSqlZoneOptionMode.class);

                if (zoneOptionMode != IgniteSqlZoneOptionMode.ALL) {
                    throw new SqlException(STMT_VALIDATION_ERR, format(
                            "Unexpected value of zone replicas [expected ALL, was {}; query=\"{}\"",
                            zoneOptionMode, ctx.query()
                    ));
                }

                // Directly set the option value and return
                replicasOptionInfo.setter.accept(target, DistributionAlgorithm.ALL_REPLICAS);
                return;
            } else {
                zoneOptionInfo = replicasOptionInfo;
            }
        }

        updateCommandOption("Zone", optionName, literal, zoneOptionInfo, ctx.query(), target);
    }

    private static <S, T> void updateCommandOption(
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

    private static  <BuilderT> void handleTablePropertyList(
            SqlNodeList propertyList,
            Map<IgniteSqlTablePropertyKey, DdlOptionInfo<BuilderT, ?>> propertyInfos,
            PlanningContext ctx,
            BuilderT target
    ) {
        Set<String> remainingKnownOptions = new HashSet<>(knownTablePropertyNames());

        for (SqlNode propertyNode : propertyList.getList()) {
            IgniteSqlTableProperty property = (IgniteSqlTableProperty) propertyNode;

            assert property != null;

            String propertyName = property.key().name();

            if (!remainingKnownOptions.remove(propertyName)) {
                throw duplicateTableProperty(ctx, property.key().sqlName);
            }

            DdlOptionInfo<BuilderT, ?> propertyInfo = propertyInfos.get(property.key());

            assert property.value() instanceof SqlLiteral : property.value();
            SqlLiteral literal = (SqlLiteral) property.value();

            updateCommandOption("Table", propertyName, literal, propertyInfo, ctx.query(), target);
        }
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

    private static List<StorageProfileParams> extractProfiles(SqlNodeList values) {
        List<StorageProfileParams> profiles = new ArrayList<>(values.size());

        SqlCharStringLiteral literal;
        for (SqlNode node : values) {
            literal = (SqlCharStringLiteral) node;
            String profile = literal.getValueAs(String.class).trim();
            profiles.add(StorageProfileParams.builder().storageProfile(profile).build());
        }

        return profiles;
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

    private static Set<String> knownTablePropertyNames() {
        return EnumSet.allOf(IgniteSqlTablePropertyKey.class)
                .stream()
                .map(Enum::name)
                .collect(Collectors.toCollection(HashSet::new));
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
        if (string.isBlank()) {
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
                    return fromInternal(val, ColumnType.PERIOD);
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
                    return fromInternal(val, ColumnType.DURATION);
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
                        return fromInternal(val, ColumnType.DATE);
                    } catch (CalciteContextException e) {
                        literal = SqlParserUtil.parseTimestampLiteral(literal.getValueAs(String.class), literal.getParserPosition());
                        TimestampString tsString = literal.getValueAs(TimestampString.class);
                        int val = convertToIntExact(TimeUnit.MILLISECONDS.toDays(tsString.getMillisSinceEpoch()));
                        return fromInternal(val, ColumnType.DATE);
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
                    return fromInternal(val, ColumnType.TIME);
                }
                case DATETIME: {
                    String sourceValue = literal.getValueAs(String.class);
                    literal = SqlParserUtil.parseTimestampLiteral(sourceValue, literal.getParserPosition());
                    var tsString = literal.getValueAs(TimestampString.class);
                    long ts = tsString.getMillisSinceEpoch();

                    if (ts < IgniteSqlFunctions.TIMESTAMP_MIN_INTERNAL
                            || ts > IgniteSqlFunctions.TIMESTAMP_MAX_INTERNAL
                            || IgniteSqlDateTimeUtils.isYearOutOfRange(sourceValue)) {
                        throw new SqlException(STMT_VALIDATION_ERR, "TIMESTAMP out of range.");
                    }

                    return fromInternal(tsString.getMillisSinceEpoch(), ColumnType.DATETIME);
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

    private static IgniteException duplicateTableProperty(PlanningContext ctx, String propertyName) {
        return new SqlException(STMT_VALIDATION_ERR,
                format("Duplicate table property has been specified [property={}, query={}]", propertyName, ctx.query()));
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
