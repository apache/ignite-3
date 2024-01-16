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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.AFFINITY_FUNCTION;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_AUTO_ADJUST;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_DOWN;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_AUTO_ADJUST_SCALE_UP;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_NODES_FILTER;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.DATA_STORAGE_ENGINE;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.PARTITIONS;
import static org.apache.ignite.internal.sql.engine.prepare.ddl.ZoneOptionEnum.REPLICAS;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUnknownLiteral;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateIndexCommand.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterTableAddColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterTableDropColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterZoneRenameTo;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterZoneSet;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateIndex;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateTable;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateTableOption;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateZone;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropIndex;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropTable;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropZone;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlIndexType;
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
    private final Supplier<String> defaultDataStorageSupplier;

    /**
     * Mapping: Data storage ID -> data storage name.
     *
     * <p>Example for "rocksdb": {@code Map.of("ROCKSDB", "rocksdb")}.
     */
    private final Map<String, String> dataStorageNames;

    /** Mapping: Table option ID -> DDL option info. */
    private final Map<String, DdlOptionInfo<CreateTableCommand, ?>> tableOptionInfos;

    /** Like {@link #tableOptionInfos}, but for each data storage name. */
    private final Map<String, Map<String, DdlOptionInfo<CreateZoneCommand, ?>>> dataStorageOptionInfos;

    /** Mapping: Zone option ID -> DDL option info. */
    private final Map<ZoneOptionEnum, DdlOptionInfo<CreateZoneCommand, ?>> zoneOptionInfos;

    /** Mapping: Zone option ID -> DDL option info. */
    private final Map<ZoneOptionEnum, DdlOptionInfo<AlterZoneSetCommand, ?>> alterZoneOptionInfos;

    /** Zone options set. */
    private final Set<String> knownZoneOptionNames;

    /**
     * Constructor.
     *
     * @param dataStorageFields Data storage fields. Mapping: Data storage name -> field name -> field type.
     * @param defaultDataStorageSupplier Default data storage supplier.
     */
    public DdlSqlToCommandConverter(
            Map<String, Map<String, Class<?>>> dataStorageFields,
            Supplier<String> defaultDataStorageSupplier
    ) {
        this.defaultDataStorageSupplier = defaultDataStorageSupplier;

        knownZoneOptionNames = EnumSet.allOf(ZoneOptionEnum.class)
                .stream()
                .map(Enum::name)
                .collect(Collectors.toSet());

        this.dataStorageNames = collectDataStorageNames(dataStorageFields.keySet());

        this.tableOptionInfos = Map.of(
                "PRIMARY_ZONE", new DdlOptionInfo<>(String.class, null, CreateTableCommand::zone)
        );

        this.dataStorageOptionInfos = dataStorageFields.entrySet()
                .stream()
                .collect(toUnmodifiableMap(
                        Entry::getKey,
                        e0 -> e0.getValue().entrySet().stream()
                                .map(this::dataStorageFieldOptionInfo)
                                .collect(toUnmodifiableMap(k -> k.getKey().toUpperCase(), Entry::getValue))
                ));

        dataStorageOptionInfos.values().forEach(v -> checkDuplicates(v.keySet(), tableOptionInfos.keySet()));

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
                DATA_STORAGE_ENGINE, new DdlOptionInfo<>(String.class, null, CreateZoneCommand::dataStorage)
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
    private CreateTableCommand convertCreateTable(IgniteSqlCreateTable createTblNode, PlanningContext ctx) {
        CreateTableCommand createTblCmd = new CreateTableCommand();

        createTblCmd.schemaName(deriveSchemaName(createTblNode.name(), ctx));
        createTblCmd.tableName(deriveObjectName(createTblNode.name(), ctx, "tableName"));
        createTblCmd.ifTableExists(createTblNode.ifNotExists());

        if (createTblNode.createOptionList() != null) {

            for (SqlNode optionNode : createTblNode.createOptionList().getList()) {
                IgniteSqlCreateTableOption option = (IgniteSqlCreateTableOption) optionNode;

                assert option.key().isSimple() : option.key();

                String optionKey = option.key().getSimple().toUpperCase();

                DdlOptionInfo<CreateTableCommand, ?> tblOptionInfo = tableOptionInfos.get(optionKey);

                if (tblOptionInfo != null) {
                    updateCommandOption("Table", optionKey, (SqlLiteral) option.value(), tblOptionInfo, ctx.query(), createTblCmd);
                } else {
                    throw new SqlException(
                            STMT_VALIDATION_ERR, String.format("Unexpected table option [option=%s, query=%s]", optionKey, ctx.query()));
                }
            }
        }

        List<SqlKeyConstraint> pkConstraints = createTblNode.columnList().getList().stream()
                .filter(SqlKeyConstraint.class::isInstance)
                .map(SqlKeyConstraint.class::cast)
                .collect(Collectors.toList());

        if (pkConstraints.isEmpty() && Commons.implicitPkEnabled()) {
            SqlIdentifier colName = new SqlIdentifier(Commons.IMPLICIT_PK_COL_NAME, SqlParserPos.ZERO);

            pkConstraints.add(SqlKeyConstraint.primary(SqlParserPos.ZERO, null, SqlNodeList.of(colName)));

            SqlDataTypeSpec type = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, SqlParserPos.ZERO), SqlParserPos.ZERO);
            SqlNode col = SqlDdlNodes.column(SqlParserPos.ZERO, colName, type, null, ColumnStrategy.DEFAULT);

            createTblNode.columnList().add(0, col);
        }

        if (nullOrEmpty(pkConstraints)) {
            throw new SqlException(STMT_VALIDATION_ERR, "Table without PRIMARY KEY is not supported");
        } else if (pkConstraints.size() > 1) {
            throw new SqlException(STMT_VALIDATION_ERR, "Unexpected amount of primary key constraints ["
                    + "expected at most one, but was " + pkConstraints.size() + "; "
                    + "querySql=\"" + ctx.query() + "\"]");
        }

        Set<String> dedupSetPk = new HashSet<>();

        List<String> pkCols = pkConstraints.stream()
                .map(pk -> pk.getOperandList().get(1))
                .map(SqlNodeList.class::cast)
                .flatMap(l -> l.getList().stream())
                .map(SqlIdentifier.class::cast)
                .map(SqlIdentifier::getSimple)
                .filter(dedupSetPk::add)
                .collect(Collectors.toList());

        createTblCmd.primaryKeyColumns(pkCols);

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

            if (col.dataType.getNullable() != null && col.dataType.getNullable() && dedupSetPk.contains(name)) {
                throw new SqlException(STMT_VALIDATION_ERR, "Primary key cannot contain nullable column [col=" + name + "]");
            }

            RelDataType relType = planner.convert(col.dataType, !dedupSetPk.contains(name));

            dedupSetPk.remove(name);

            DefaultValueDefinition dflt = convertDefault(col.expression, relType);
            if (dflt.type() == DefaultValueDefinition.Type.FUNCTION_CALL && !pkCols.contains(name)) {
                throw new SqlException(STMT_VALIDATION_ERR,
                        "Functional defaults are not supported for non-primary key columns [col=" + name + "]");
            }

            cols.add(new ColumnDefinition(name, relType, dflt));
        }

        if (!dedupSetPk.isEmpty()) {
            throw new SqlException(STMT_VALIDATION_ERR, "Primary key constraint contains undefined columns: [cols=" + dedupSetPk + "]");
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
            DefaultValueDefinition dflt = convertDefault(col.expression, relType);

            String name = col.name.getSimple();

            cols.add(new ColumnDefinition(name, relType, dflt));
        }

        alterTblCmd.columns(cols);

        return alterTblCmd;
    }

    private static DefaultValueDefinition convertDefault(@Nullable SqlNode expression, RelDataType relType) {
        if (expression == null) {
            return DefaultValueDefinition.constant(null);
        } else if (expression instanceof SqlIdentifier) {
            return DefaultValueDefinition.functionCall(((SqlIdentifier) expression).getSimple());
        } else if (expression instanceof SqlLiteral) {
            ColumnType columnType = TypeUtils.columnType(relType);
            assert columnType != null : "RelType to columnType conversion should not return null";

            Object val = fromLiteral(columnType, (SqlLiteral) expression);
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

            if (expr instanceof SqlLiteral) {
                resolveDfltFunc = type -> DefaultValue.constant(fromLiteral(type, (SqlLiteral) expr));
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

        List<String> columns = new ArrayList<>(sqlCmd.columnList().size());
        List<Collation> collations = new ArrayList<>(sqlCmd.columnList().size());

        for (SqlNode col : sqlCmd.columnList().getList()) {
            boolean desc = false;

            if (col.getKind() == SqlKind.DESCENDING) {
                col = ((SqlCall) col).getOperandList().get(0);

                desc = true;
            }

            columns.add(((SqlIdentifier) col).getSimple());
            collations.add(desc ? Collation.DESC_NULLS_FIRST : Collation.ASC_NULLS_LAST);
        }

        createIdxCmd.columns(columns);

        if (createIdxCmd.type() == Type.SORTED) {
            createIdxCmd.collations(collations);
        }

        createIdxCmd.ifNotExists(sqlCmd.ifNotExists());

        return createIdxCmd;
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
        String dataStorageName = deriveDataStorage(createZoneNode.engineName(), ctx);

        createZoneCmd.schemaName(deriveSchemaName(createZoneNode.name(), ctx));
        createZoneCmd.zoneName(deriveObjectName(createZoneNode.name(), ctx, "zoneName"));
        createZoneCmd.ifNotExists(createZoneNode.ifNotExists());
        createZoneCmd.dataStorage(dataStorageName);

        if (createZoneNode.createOptionList() == null) {
            return createZoneCmd;
        }

        Map<String, DdlOptionInfo<CreateZoneCommand, ?>> dsOptInfos = dataStorageOptionInfos.get(dataStorageName);

        Set<String> remainingKnownOptions = new HashSet<>(knownZoneOptionNames);

        for (SqlNode optionNode : createZoneNode.createOptionList().getList()) {
            IgniteSqlZoneOption option = (IgniteSqlZoneOption) optionNode;

            assert option.key().isSimple() : option.key();

            String optionName = option.key().getSimple().toUpperCase();

            DdlOptionInfo<CreateZoneCommand, ?> zoneOptionInfo;

            if (remainingKnownOptions.remove(optionName)) {
                zoneOptionInfo = zoneOptionInfos.get(ZoneOptionEnum.valueOf(optionName));
            } else if (knownZoneOptionNames.contains(optionName)) {
                throw duplicateZoneOption(ctx, optionName);
            } else {
                zoneOptionInfo = dsOptInfos != null ? dsOptInfos.get(optionName) : null;
            }

            if (zoneOptionInfo == null) {
                throw unexpectedZoneOption(ctx, optionName);
            }

            updateCommandOption("Zone", optionName, (SqlLiteral) option.value(), zoneOptionInfo, ctx.query(), createZoneCmd);
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
    private DropZoneCommand convertDropZone(IgniteSqlDropZone dropZoneNode, PlanningContext ctx) {
        DropZoneCommand dropZoneCmd = new DropZoneCommand();

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

    /**
     * Collects a mapping of the ID of the data storage to a name.
     *
     * <p>Example: {@code collectDataStorageNames(Set.of("rocksdb"))} -> {@code Map.of("ROCKSDB", "rocksdb")}.
     *
     * @param dataStorages Names of the data storages.
     * @throws IllegalStateException If there is a duplicate ID.
     */
    static Map<String, String> collectDataStorageNames(Set<String> dataStorages) {
        return dataStorages.stream().collect(toUnmodifiableMap(String::toUpperCase, identity()));
    }

    /**
     * Checks that there are no ID duplicates.
     *
     * @param set0 Set of string identifiers.
     * @param set1 Set of string identifiers.
     * @throws IllegalStateException If there is a duplicate ID.
     */
    static void checkDuplicates(Set<String> set0, Set<String> set1) {
        for (String id : set1) {
            if (set0.contains(id)) {
                throw new IllegalStateException("Duplicate id: " + id);
            }
        }
    }

    private String deriveDataStorage(@Nullable SqlIdentifier engineName, PlanningContext ctx) {
        if (engineName == null) {
            String defaultDataStorage = defaultDataStorageSupplier.get();

            return defaultDataStorage;
        }

        assert engineName.isSimple() : engineName;

        String dataStorage = engineName.getSimple().toUpperCase();

        if (!dataStorageNames.containsKey(dataStorage)) {
            throw new SqlException(STMT_VALIDATION_ERR, String.format(
                    "Unexpected data storage engine [engine=%s, expected=%s, query=%s]",
                    dataStorage, dataStorageNames, ctx.query()
            ));
        }

        return dataStorageNames.get(dataStorage);
    }

    private <S, T> void updateCommandOption(
            String sqlObjName,
            Object optId,
            SqlLiteral value,
            DdlOptionInfo<S, T> optInfo,
            String query,
            S target
    ) {
        T value0;

        try {
            value0 = value.getValueAs(optInfo.type);
        } catch (Throwable e) {
            throw new SqlException(STMT_VALIDATION_ERR, String.format(
                    "Invalid %s option type [option=%s, expectedType=%s, query=%s]",
                    sqlObjName.toLowerCase(),
                    optId,
                    optInfo.type.getSimpleName(),
                    query)
            );
        }

        if (optInfo.validator != null) {
            try {
                optInfo.validator.accept(value0);
            } catch (Throwable e) {
                throw new SqlException(STMT_VALIDATION_ERR, String.format(
                        "%s option validation failed [option=%s, err=%s, query=%s]",
                        sqlObjName,
                        optId,
                        e.getMessage(),
                        query
                ), e);
            }
        }

        optInfo.setter.accept(target, value0);
    }

    private void checkPositiveNumber(int num) {
        if (num < 0) {
            throw new SqlException(STMT_VALIDATION_ERR, "Must be positive:" + num);
        }
    }

    private Entry<String, DdlOptionInfo<CreateZoneCommand, ?>> dataStorageFieldOptionInfo(Entry<String, Class<?>> e) {
        return new SimpleEntry<>(
                e.getKey(),
                new DdlOptionInfo<>(e.getValue(), null, (cmd, o) -> cmd.addDataStorageOption(e.getKey(), o))
        );
    }

    private Type convertIndexType(IgniteSqlIndexType type) {
        switch (type) {
            case TREE:
            case IMPLICIT_TREE:
                return Type.SORTED;
            case HASH:
                return Type.HASH;
            default:
                throw new AssertionError("Unknown index type [type=" + type + "]");
        }
    }

    /**
     * Creates a value of required type from the literal.
     */
    private static @Nullable Object fromLiteral(ColumnType columnType, SqlLiteral literal) {
        if (literal.getValue() == null) {
            return null;
        }

        try {
            switch (columnType) {
                case STRING:
                    return literal.getValueAs(String.class);
                case DATE: {
                    SqlLiteral literal0 = ((SqlUnknownLiteral) literal).resolve(SqlTypeName.DATE);
                    return LocalDate.ofEpochDay(literal0.getValueAs(DateString.class).getDaysSinceEpoch());
                }
                case TIME: {
                    SqlLiteral literal0 = ((SqlUnknownLiteral) literal).resolve(SqlTypeName.TIME);
                    return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(literal0.getValueAs(TimeString.class).getMillisOfDay()));
                }
                case DATETIME: {
                    SqlLiteral literal0 = ((SqlUnknownLiteral) literal).resolve(SqlTypeName.TIMESTAMP);
                    var tsString = literal0.getValueAs(TimestampString.class);

                    return LocalDateTime.ofEpochSecond(
                            TimeUnit.MILLISECONDS.toSeconds(tsString.getMillisSinceEpoch()),
                            (int) (TimeUnit.MILLISECONDS.toNanos(tsString.getMillisSinceEpoch() % 1000)),
                            ZoneOffset.UTC
                    );
                }
                case TIMESTAMP:
                    // TODO: IGNITE-17376
                    throw new UnsupportedOperationException("Type is not supported: " + columnType);
                case INT32:
                    return literal.getValueAs(Integer.class);
                case INT64:
                    return literal.getValueAs(Long.class);
                case INT16:
                    return literal.getValueAs(Short.class);
                case INT8:
                    return literal.getValueAs(Byte.class);
                case DECIMAL:
                    return literal.getValueAs(BigDecimal.class);
                case DOUBLE:
                    return literal.getValueAs(Double.class);
                case FLOAT:
                    return literal.getValueAs(Float.class);
                case BYTE_ARRAY:
                    return literal.getValueAs(byte[].class);
                case BOOLEAN:
                    return literal.getValueAs(Boolean.class);
                default:
                    throw new IllegalStateException("Unknown type [type=" + columnType + ']');
            }
        } catch (Throwable th) {
            // catch throwable here because literal throws an AssertionError when unable to cast value to a given class
            throw new SqlException(STMT_VALIDATION_ERR, "Unable convert literal", th);
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
