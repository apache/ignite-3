/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema.UNKNOWN_DATA_STORAGE;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlDdlNodes;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.prepare.IgnitePlanner;
import org.apache.ignite.internal.sql.engine.prepare.PlanningContext;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterTableAddColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlAlterTableDropColumn;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateIndex;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateTable;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlCreateTableOption;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlDropIndex;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.IgniteException;
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

    /**
     * Mapping: Table option ID -> table option info.
     *
     * <p>Example for "replicas": {@code Map.of("REPLICAS", TableOptionInfo@123)}.
     */
    private final Map<String, TableOptionInfo<?>> tableOptionInfos;

    /**
     * Like {@link #tableOptionInfos}, but for each data storage name.
     */
    private final Map<String, Map<String, TableOptionInfo<?>>> dataStorageOptionInfos;

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

        this.dataStorageNames = collectDataStorageNames(dataStorageFields.keySet());

        this.tableOptionInfos = collectTableOptionInfos(
                new TableOptionInfo<>("replicas", Integer.class, this::checkPositiveNumber, CreateTableCommand::replicas),
                new TableOptionInfo<>("partitions", Integer.class, this::checkPositiveNumber, CreateTableCommand::partitions)
        );

        this.dataStorageOptionInfos = dataStorageFields.entrySet()
                .stream()
                .collect(toUnmodifiableMap(
                        Entry::getKey,
                        e0 -> collectTableOptionInfos(
                                e0.getValue().entrySet().stream()
                                        .map(this::dataStorageFieldOptionInfo)
                                        .toArray(TableOptionInfo[]::new)
                        )
                ));

        dataStorageOptionInfos.forEach((k, v) -> checkDuplicates(v, tableOptionInfos));
    }

    /**
     * Converts a given ddl AST to a ddl command.
     *
     * @param ddlNode Root node of the given AST.
     * @param ctx     Planning context.
     */
    public DdlCommand convert(SqlDdl ddlNode, PlanningContext ctx) {
        if (ddlNode instanceof IgniteSqlCreateTable) {
            return convertCreateTable((IgniteSqlCreateTable) ddlNode, ctx);
        }

        if (ddlNode instanceof SqlDropTable) {
            return convertDropTable((SqlDropTable) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlAlterTableAddColumn) {
            return convertAlterTableAdd((IgniteSqlAlterTableAddColumn) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlAlterTableDropColumn) {
            return convertAlterTableDrop((IgniteSqlAlterTableDropColumn) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlCreateIndex) {
            return convertAddIndex((IgniteSqlCreateIndex) ddlNode, ctx);
        }

        if (ddlNode instanceof IgniteSqlDropIndex) {
            return convertDropIndex((IgniteSqlDropIndex) ddlNode);
        }

        throw new IgniteException("Unsupported operation ["
                + "sqlNodeKind=" + ddlNode.getKind() + "; "
                + "querySql=\"" + ctx.query() + "\"]");
    }

    /**
     * Converts a given CreateTable AST to a CreateTable command.
     *
     * @param createTblNode Root node of the given AST.
     * @param ctx           Planning context.
     */
    private CreateTableCommand convertCreateTable(IgniteSqlCreateTable createTblNode, PlanningContext ctx) {
        CreateTableCommand createTblCmd = new CreateTableCommand();

        createTblCmd.schemaName(deriveSchemaName(createTblNode.name(), ctx));
        createTblCmd.tableName(deriveObjectName(createTblNode.name(), ctx, "tableName"));
        createTblCmd.ifTableExists(createTblNode.ifNotExists());
        createTblCmd.dataStorage(deriveDataStorage(createTblNode.engineName(), ctx));

        if (createTblNode.createOptionList() != null) {
            for (SqlNode optionNode : createTblNode.createOptionList().getList()) {
                IgniteSqlCreateTableOption option = (IgniteSqlCreateTableOption) optionNode;

                assert option.key().isSimple() : option.key();

                String optionKey = option.key().getSimple().toUpperCase();

                if (tableOptionInfos.containsKey(optionKey)) {
                    processTableOption(tableOptionInfos.get(optionKey), option, ctx, createTblCmd);
                } else if (dataStorageOptionInfos.get(createTblCmd.dataStorage()).containsKey(optionKey)) {
                    processTableOption(dataStorageOptionInfos.get(createTblCmd.dataStorage()).get(optionKey), option, ctx, createTblCmd);
                } else {
                    throw new IgniteException(String.format("Unexpected table option [option=%s, query=%s]", optionKey, ctx.query()));
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
            throw new IgniteException("Table without PRIMARY KEY is not supported");
        } else if (pkConstraints.size() > 1) {
            throw new IgniteException("Unexpected amount of primary key constraints ["
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
                throw new IgniteException("Unexpected value of columnName ["
                        + "expected a simple identifier, but was " + col.name + "; "
                        + "querySql=\"" + ctx.query() + "\"]");
            }

            String name = col.name.getSimple();

            if (col.dataType.getNullable() != null && col.dataType.getNullable() && dedupSetPk.contains(name)) {
                throw new IgniteException("Primary key cannot contain nullable column [col=" + name + "]");
            }

            RelDataType relType = planner.convert(col.dataType, !dedupSetPk.contains(name));

            dedupSetPk.remove(name);

            Object dflt = null;
            if (col.expression != null) {
                dflt = ((SqlLiteral) col.expression).getValue();
            }

            cols.add(new ColumnDefinition(name, relType, dflt));
        }

        if (!dedupSetPk.isEmpty()) {
            throw new IgniteException("Primary key constrain contains undefined columns: [cols=" + dedupSetPk + "]");
        }

        createTblCmd.columns(cols);

        return createTblCmd;
    }

    /**
     * Converts a given IgniteSqlAlterTableAddColumn AST to a AlterTableAddCommand.
     *
     * @param alterTblNode Root node of the given AST.
     * @param ctx          Planning context.
     */
    private AlterTableAddCommand convertAlterTableAdd(IgniteSqlAlterTableAddColumn alterTblNode, PlanningContext ctx) {
        AlterTableAddCommand alterTblCmd = new AlterTableAddCommand();

        alterTblCmd.schemaName(deriveSchemaName(alterTblNode.name(), ctx));
        alterTblCmd.tableName(deriveObjectName(alterTblNode.name(), ctx, "table name"));
        alterTblCmd.ifTableExists(alterTblNode.ifExists());
        alterTblCmd.ifColumnNotExists(alterTblNode.ifNotExistsColumn());

        List<ColumnDefinition> cols = new ArrayList<>(alterTblNode.columns().size());

        for (SqlNode colNode : alterTblNode.columns()) {
            assert colNode instanceof SqlColumnDeclaration : colNode.getClass();

            SqlColumnDeclaration col = (SqlColumnDeclaration) colNode;

            assert col.name.isSimple();

            Object dflt = null;
            if (col.expression != null) {
                dflt = ((SqlLiteral) col.expression).getValue();
            }

            String name = col.name.getSimple();
            RelDataType relType = ctx.planner().convert(col.dataType, true);

            cols.add(new ColumnDefinition(name, relType, dflt));
        }

        alterTblCmd.columns(cols);

        return alterTblCmd;
    }

    /**
     * Converts a given IgniteSqlAlterTableDropColumn AST to a AlterTableDropCommand.
     *
     * @param alterTblNode Root node of the given AST.
     * @param ctx          Planning context.
     */
    private AlterTableDropCommand convertAlterTableDrop(IgniteSqlAlterTableDropColumn alterTblNode, PlanningContext ctx) {
        AlterTableDropCommand alterTblCmd = new AlterTableDropCommand();

        alterTblCmd.schemaName(deriveSchemaName(alterTblNode.name(), ctx));
        alterTblCmd.tableName(deriveObjectName(alterTblNode.name(), ctx, "table name"));
        alterTblCmd.ifTableExists(alterTblNode.ifExists());
        alterTblCmd.ifColumnExists(alterTblNode.ifExistsColumn());

        Set<String> cols = new HashSet<>(alterTblNode.columns().size());
        alterTblNode.columns().forEach(c -> cols.add(((SqlIdentifier) c).getSimple()));

        alterTblCmd.columns(cols);

        return alterTblCmd;
    }

    /**
     * Converts a given DropTable AST to a DropTable command.
     *
     * @param dropTblNode Root node of the given AST.
     * @param ctx         Planning context.
     */
    private DropTableCommand convertDropTable(SqlDropTable dropTblNode, PlanningContext ctx) {
        DropTableCommand dropTblCmd = new DropTableCommand();

        dropTblCmd.schemaName(deriveSchemaName(dropTblNode.name, ctx));
        dropTblCmd.tableName(deriveObjectName(dropTblNode.name, ctx, "tableName"));
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

        List<Pair<String, Boolean>> cols = new ArrayList<>(sqlCmd.columnList().size());

        for (SqlNode col : sqlCmd.columnList().getList()) {
            boolean desc = false;

            if (col.getKind() == SqlKind.DESCENDING) {
                col = ((SqlCall) col).getOperandList().get(0);

                desc = true;
            }

            cols.add(new Pair<>(((SqlIdentifier) col).getSimple(), desc));
        }

        createIdxCmd.columns(cols);

        createIdxCmd.ifIndexNotExists(sqlCmd.ifNotExists());

        return createIdxCmd;
    }

    /**
     * Converts drop index to appropriate wrapper.
     */
    private DropIndexCommand convertDropIndex(IgniteSqlDropIndex sqlCmd) {
        DropIndexCommand dropCmd = new DropIndexCommand();

        dropCmd.indexName(sqlCmd.idxName().getSimple());
        dropCmd.ifExist(sqlCmd.ifExists());

        return dropCmd;
    }

    /** Derives a schema name from the compound identifier. */
    private String deriveSchemaName(SqlIdentifier id, PlanningContext ctx) {
        String schemaName;
        if (id.isSimple()) {
            schemaName = ctx.schemaName();
        } else {
            SqlIdentifier schemaId = id.skipLast(1);

            if (!schemaId.isSimple()) {
                throw new IgniteException("Unexpected value of schemaName ["
                        + "expected a simple identifier, but was " + schemaId + "; "
                        + "querySql=\"" + ctx.query() + "\"]"/*, IgniteQueryErrorCode.PARSING*/);
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
            throw new IgniteException("Unexpected value of " + objDesc + " ["
                    + "expected a simple identifier, but was " + objId + "; "
                    + "querySql=\"" + ctx.query() + "\"]"/*, IgniteQueryErrorCode.PARSING*/);
        }

        return objId.getSimple();
    }

    private void ensureSchemaExists(PlanningContext ctx, String schemaName) {
        if (ctx.catalogReader().getRootSchema().getSubSchema(schemaName, true) == null) {
            throw new IgniteException("Schema with name " + schemaName + " not found"/*,
                IgniteQueryErrorCode.SCHEMA_NOT_FOUND*/);
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
     * Collects a mapping of the ID of the table option to a table option info.
     *
     * <p>Example for "replicas": {@code Map.of("REPLICAS", TableOptionInfo@123)}.
     *
     * @param tableOptionInfos Table option information's.
     * @throws IllegalStateException If there is a duplicate ID.
     */
    static Map<String, TableOptionInfo<?>> collectTableOptionInfos(TableOptionInfo<?>... tableOptionInfos) {
        return ArrayUtils.nullOrEmpty(tableOptionInfos) ? Map.of() : Stream.of(tableOptionInfos).collect(toUnmodifiableMap(
                tableOptionInfo -> tableOptionInfo.name.toUpperCase(),
                identity()
        ));
    }

    /**
     * Checks that there are no ID duplicates.
     *
     * @param tableOptionInfos0 Table options information.
     * @param tableOptionInfos1 Table options information.
     * @throws IllegalStateException If there is a duplicate ID.
     */
    static void checkDuplicates(Map<String, TableOptionInfo<?>> tableOptionInfos0, Map<String, TableOptionInfo<?>> tableOptionInfos1) {
        for (String id : tableOptionInfos1.keySet()) {
            if (tableOptionInfos0.containsKey(id)) {
                throw new IllegalStateException("Duplicate id:" + id);
            }
        }
    }

    private String deriveDataStorage(@Nullable SqlIdentifier engineName, PlanningContext ctx) {
        if (engineName == null) {
            String defaultDataStorage = defaultDataStorageSupplier.get();

            if (defaultDataStorage.equals(UNKNOWN_DATA_STORAGE)) {
                throw new IgniteException("Default data storage is not defined, query:" + ctx.query());
            }

            return defaultDataStorage;
        }

        assert engineName.isSimple() : engineName;

        String dataStorage = engineName.getSimple().toUpperCase();

        if (!dataStorageNames.containsKey(dataStorage)) {
            throw new IgniteException(String.format(
                    "Unexpected data storage engine [engine=%s, expected=%s, query=%s]",
                    dataStorage, dataStorageNames, ctx.query()
            ));
        }

        return dataStorageNames.get(dataStorage);
    }

    private void processTableOption(
            TableOptionInfo tableOptionInfo,
            IgniteSqlCreateTableOption option,
            PlanningContext context,
            CreateTableCommand createTableCommand
    ) {
        assert option.value() instanceof SqlLiteral : option.value();

        Object optionValue;

        try {
            optionValue = ((SqlLiteral) option.value()).getValueAs(tableOptionInfo.type);
        } catch (AssertionError | ClassCastException e) {
            throw new IgniteException(String.format(
                    "Unsuspected table option type [option=%s, expectedType=%s, query=%s]",
                    option.key().getSimple(),
                    tableOptionInfo.type.getSimpleName(),
                    context.query()
            ));
        }

        if (tableOptionInfo.validator != null) {
            try {
                tableOptionInfo.validator.accept(optionValue);
            } catch (Throwable e) {
                throw new IgniteException(String.format(
                        "Table option validation failed [option=%s, err=%s, query=%s]",
                        option.key().getSimple(),
                        e.getMessage(),
                        context.query()
                ), e);
            }
        }

        tableOptionInfo.setter.accept(createTableCommand, optionValue);
    }

    private void checkPositiveNumber(int num) {
        if (num < 0) {
            throw new IgniteException("Must be positive:" + num);
        }
    }

    private TableOptionInfo<?> dataStorageFieldOptionInfo(Entry<String, Class<?>> e) {
        return new TableOptionInfo<>(e.getKey(), e.getValue(), null, (cmd, o) -> cmd.addDataStorageOption(e.getKey(), o));
    }
}
