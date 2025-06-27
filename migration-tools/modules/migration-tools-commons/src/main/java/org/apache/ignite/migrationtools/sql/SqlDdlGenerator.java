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

package org.apache.ignite.migrationtools.sql;

import static java.util.function.Predicate.not;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.migrationtools.tablemanagement.TableTypeRegistry;
import org.apache.ignite.migrationtools.tablemanagement.TableTypeRegistryMapImpl;
import org.apache.ignite.migrationtools.utils.ClassnameUtils;
import org.apache.ignite3.catalog.ColumnType;
import org.apache.ignite3.catalog.definitions.ColumnDefinition;
import org.apache.ignite3.catalog.definitions.TableDefinition;
import org.apache.ignite3.internal.catalog.sql.CatalogExtensions;
import org.apache.ignite3.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a SQL DDL Script from Ignite 2 Cache Configurations and custom type hints.
 */
public class SqlDdlGenerator {
    /** Name of the column that will be used to store fields that cannot be serialized natively, like nested objects in POJOs. */
    public static final String EXTRA_FIELDS_COLUMN_NAME = "__EXTRA__";

    private static final String[] VALUE_FIELD_NAME_CANDIDATES = new String[] {"VAL", "VAL_OBJ", "TARGET_OBJ"};

    private static final String[] ID_FIELD_NAME_CANDIDATES = new String[] {"ID", "KEY"};

    // Extracted from org.apache.ignite.internal.sql.engine.sql.SqlReservedWordsTest#RESERVED_WORDS

    /** List of keywords reserved in Ignite SQL. */
    private static final Set<String> RESERVED_WORDS = Set.of(
            "ABS",
            "ALL", // UNION ALL
            "ALTER",
            "AND",
            "ANY",
            "ARRAY",
            "ARRAY_MAX_CARDINALITY",
            "AS",
            "ASYMMETRIC", // BETWEEN ASYMMETRIC .. AND ..
            "AVG",
            "BETWEEN",
            "BOTH", // TRIM(BOTH .. FROM ..)
            "BY", // GROUP BY
            "CACHE",
            "CALL",
            "CARDINALITY",
            "CASE",
            "CAST",
            "CEILING",
            "CHAR",
            "CHARACTER",
            "CHARACTER_LENGTH",
            "CHAR_LENGTH",
            "COALESCE",
            "COLLECT",
            "COLUMN",
            "CONSTRAINT",
            "CONVERT",
            "COUNT",
            "COVAR_POP",
            "COVAR_SAMP",
            "CREATE",
            "CROSS", // CROSS JOIN
            "CUBE",
            "CUME_DIST",
            "CURRENT",
            "CURRENT_CATALOG",
            "CURRENT_DATE",
            "CURRENT_DEFAULT_TRANSFORM_GROUP",
            "CURRENT_PATH",
            "CURRENT_ROLE",
            "CURRENT_ROW",
            "CURRENT_SCHEMA",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
            "CURRENT_USER",
            "DATE",
            "DATETIME",
            "DECIMAL",
            "DEFAULT",
            "DELETE",
            "DENSE_RANK",
            "DESCRIBE",
            "DISTINCT",
            "DROP",
            "ELEMENT",
            "ELSE",
            "EVERY",
            "EXCEPT",
            "EXISTS",
            "EXP",
            "EXPLAIN",
            "EXTEND",
            "EXTRACT",
            "FALSE",
            "FETCH",
            "FILTER",
            "FIRST_VALUE",
            "FLOOR",
            "FOR", // SUBSTRING(.. FROM .. FOR ..)
            "FRIDAY",
            "FROM",
            "FULL", // FULL JOIN
            "FUSION",
            "GRANTS",
            "GROUP",
            "GROUPING",
            "HAVING",
            "HOUR",
            "IDENTIFIED",
            "IF",
            "IN",
            "INDEX",
            "INNER",
            "INSERT",
            "INTERSECT",
            "INTERSECTION",
            "INTERVAL",
            "INTO",
            "IS",
            "JOIN",
            "JSON_SCOPE",
            "LAG",
            "LAST_VALUE",
            "LEAD",
            "LEADING", // TRIM(LEADING .. FROM ..)
            "LEFT", // LEFT JOIN
            "LIKE",
            "LIMIT",
            "LN",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "LOWER",
            "MATCH_RECOGNIZE",
            "MAX",
            "MERGE",
            "MIN",
            "MINUS",
            "MINUTE",
            "MOD",
            "MONDAY",
            "MONTH",
            "MULTISET",
            "NATURAL", // NATURAL JOIN
            "NEW",
            "NEXT",
            "NOT",
            "NTH_VALUE",
            "NTILE",
            "NULL",
            "NULLIF",
            "OCTET_LENGTH",
            "OFFSET",
            "ON",
            "OR",
            "ORDER",
            "OUTER", // OUTER JOIN
            "OVER",
            "PARTITION",
            "PERCENTILE_CONT",
            "PERCENTILE_DISC",
            "PERCENT_RANK",
            "PERIOD",
            "PERMUTE",
            "POWER",
            "PRECISION",
            "PRIMARY",
            "QUALIFY",
            "RANK",
            "REGR_COUNT",
            "REGR_SXX",
            "REGR_SYY",
            "RENAME",
            "RESET",
            "RIGHT",
            "ROLES",
            "ROLLUP",
            "ROW",
            "ROW_NUMBER",
            "SATURDAY",
            "SECOND",
            "SELECT",
            "SESSION_USER",
            "SET",
            "SOME",
            "SPECIFIC",
            "SQRT",
            "STDDEV_POP",
            "STDDEV_SAMP",
            "STREAM",
            "SUBSTRING",
            "SUM",
            "SUNDAY",
            "SYMMETRIC", // BETWEEN SYMMETRIC .. AND ..
            "SYSTEM_TIME",
            "SYSTEM_USER",
            "TABLE",
            "TABLESAMPLE",
            "THEN",
            "THURSDAY",
            "TIME",
            "TIMESTAMP",
            "TO",
            "TRAILING", // TRIM(TRAILING .. FROM ..)
            "TRUE",
            "TRUNCATE",
            "TUESDAY",
            "UESCAPE",
            "UNION",
            "UNKNOWN",
            "UPDATE",
            "UPPER",
            "UPSERT",
            "USER",
            "USERS",
            "USING",
            "VALUE",
            "VALUES",
            "VAR_POP",
            "VAR_SAMP",
            "WEDNESDAY",
            "WHEN",
            "WHERE",
            "WINDOW",
            "WITH",
            "WITHIN",
            "YEAR"
    );

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDdlGenerator.class);

    private static final String DEFAULT_SCHEMA = "PUBLIC";

    private static final int DEFAULT_BINARY_FIELD_LENGTH = 1024;

    private static final Map<Class<?>, ColumnType<?>> COL_TYPE_REF;

    static {
        try {
            COL_TYPE_REF = (Map<Class<?>, ColumnType<?>>) FieldUtils.readDeclaredStaticField(ColumnType.class, "TYPES", true);
            // TODO: IGNITE-23268 Remove
            COL_TYPE_REF.remove(java.util.Date.class);

            var constructor = ColumnType.class.getDeclaredConstructor(Class.class, String.class);
            constructor.setAccessible(true);
            constructor.newInstance(Character.class, "CHAR");
            constructor.newInstance(BitSet.class, "VARBINARY");
            constructor.newInstance(LocalTime.class, "TIME");
            constructor.newInstance(LocalDate.class, "DATE");
            constructor.newInstance(LocalDateTime.class, "TIMESTAMP");
            constructor.newInstance(Instant.class, "TIMESTAMP");
            constructor.newInstance(java.util.Date.class, "TIMESTAMP");
            constructor.newInstance(Enum.class, "VARCHAR");
            // TODO: IGNITE-23268 Remove
            constructor.newInstance(java.sql.Date.class, "DATE");
            constructor.newInstance(java.sql.Time.class, "TIME");
            constructor.newInstance(java.sql.Timestamp.class, "TIMESTAMP");
            // Collections
            constructor.newInstance(Collection.class, "VARBINARY");
            constructor.newInstance(List.class, "VARBINARY");
            constructor.newInstance(Set.class, "VARBINARY");
            // Primitive Arrays
            constructor.newInstance(boolean[].class, "VARBINARY");
            constructor.newInstance(char[].class, "VARBINARY");
            constructor.newInstance(short[].class, "VARBINARY");
            constructor.newInstance(int[].class, "VARBINARY");
            constructor.newInstance(long[].class, "VARBINARY");
            constructor.newInstance(float[].class, "VARBINARY");
            constructor.newInstance(double[].class, "VARBINARY");
            constructor.newInstance(String[].class, "VARBINARY");
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    private final ClassLoader clientClassLoader;

    private final TableTypeRegistry tableTypeRegistry;

    private final boolean allowExtraFields;

    public SqlDdlGenerator() {
        this(new TableTypeRegistryMapImpl());
    }

    public SqlDdlGenerator(TableTypeRegistry tableTypeRegistry) {
        this(tableTypeRegistry, false);
    }

    public SqlDdlGenerator(TableTypeRegistry tableTypeRegistry, boolean allowExtraFields) {
        this(SqlDdlGenerator.class.getClassLoader(), tableTypeRegistry, allowExtraFields);
    }

    /**
     * Constructor.
     *
     * @param clientClassLoader Custom classloader to use.
     * @param tableTypeRegistry Table type registry implementation to use.
     * @param allowExtraFields If true, the extra fields column will be added to generated tables.
     */
    public SqlDdlGenerator(ClassLoader clientClassLoader, TableTypeRegistry tableTypeRegistry, boolean allowExtraFields) {
        this.clientClassLoader = clientClassLoader;
        this.tableTypeRegistry = tableTypeRegistry;
        this.allowExtraFields = allowExtraFields;
    }

    private static boolean isPrimitiveType(Class<?> type) {
        return type.isEnum() || Mapper.nativelySupported(type) || COL_TYPE_REF.containsKey(type);
    }

    private static List<InspectedField> inspectType(Class<?> type) {
        Class<?> rootType = ClassUtils.primitiveToWrapper(type);
        String rootTypeName = rootType.getName();

        if (rootType.isArray() || Collection.class.isAssignableFrom(rootType)) {
            return Collections.singletonList(new InspectedField(null, rootTypeName, FieldType.ARRAY));
        } else if (isPrimitiveType(rootType)) {
            return Collections.singletonList(new InspectedField(null, rootTypeName, FieldType.PRIMITIVE));
        } else {
            Field[] fields = rootType.getDeclaredFields();
            List<InspectedField> ret = new ArrayList<>(fields.length);
            for (Field field : fields) {
                Class<?> origFieldType = field.getType();
                Class<?> wrappedFieldType = ClassUtils.primitiveToWrapper(origFieldType);
                if (shouldPersistField(field) && isPrimitiveType(wrappedFieldType)) {
                    boolean nullable = !origFieldType.isPrimitive();
                    ret.add(new InspectedField(field.getName(), wrappedFieldType.getName(), FieldType.POJO_ATTRIBUTE, nullable));
                }
            }

            return ret;
        }
    }

    private static void addToQueryEntity(QueryEntity qe, InspectedField inspectedField, boolean nullable) {
        // Set precision
        if (inspectedField.fieldType == FieldType.ARRAY) {
            qe.getFieldsPrecision().putIfAbsent(inspectedField.fieldName, DEFAULT_BINARY_FIELD_LENGTH);
        }

        if (!nullable) {
            qe.getNotNullFields().add(inspectedField.fieldName);
        }

        qe.getFields().put(inspectedField.fieldName, inspectedField.typeName);
    }

    private static void addToQueryEntity(QueryEntity qe, InspectedField inspectedField) {
        addToQueryEntity(qe, inspectedField, inspectedField.isNullable());
    }

    private static boolean shouldPersistField(Field field) {
        var mods = field.getModifiers();
        return !Modifier.isStatic(mods) && !Modifier.isTransient(mods);
    }

    private static String sanitizeColumnName(String columnName) {
        // If is quoted the field is accepted as is.
        if (columnName.startsWith("\"")) {
            return columnName;
        }

        String uppercaseColumnName = columnName.toUpperCase();
        // If the column name is reserved then we must quote it.
        return (RESERVED_WORDS.contains(uppercaseColumnName)) ? '"' + uppercaseColumnName + '"' : columnName;
    }

    public static String createDdlQuery(TableDefinition def) {
        return createDdlQuery(Collections.singletonList(def));
    }

    /**
     * Create SQL DDL Statements from the provided list of table definitions.
     *
     * @param defs Table definitions.
     * @return String composed by the SQL DDL Statements.
     */
    public static String createDdlQuery(List<TableDefinition> defs) {
        return defs.stream()
                .map(CatalogExtensions::sqlFromTableDefinition)
                .collect(Collectors.joining("\n\n"));
    }

    @Nullable
    private static Map.Entry<String, String> keyField(QueryEntity qe) {
        String fname = qe.getKeyFieldName();
        return (fname != null) ? Map.entry(fname, qe.getKeyType()) : null;
    }

    @Nullable
    private static Map.Entry<String, String> valField(QueryEntity qe) {
        String fname = qe.getValueFieldName();
        return (fname != null) ? Map.entry(fname, qe.getValueType()) : null;
    }

    private List<InspectedField> inspectTypeName(String typeName, String typeDescr) {
        try {
            Class<?> type = ClassUtils.getClass(this.clientClassLoader, typeName);
            return inspectType(type);
        } catch (ClassNotFoundException e) {
            LOGGER.warn("Could not find {} class to enrich the QueryEntity: {}", typeDescr, typeName);
            return Collections.emptyList();
        }
    }

    private QueryEntity populateQueryEntity(QueryEntity qe, boolean allowExtraFields) throws FieldNameConflictException {
        // Make sure QE has non-null maps
        {
            if (qe.getNotNullFields() == null) {
                qe.setNotNullFields(new HashSet<>());
            }

            if (qe.getKeyFields() == null) {
                qe.setKeyFields(new HashSet<>());
            }

            if (qe.getFieldsPrecision() == null) {
                qe.setFieldsPrecision(new HashMap<>());
            }
        }

        // Go over existing fields in the QE to they are correct and there are not silly nulls.
        {
            // AI2 may define primitive field types, however, they still mark them as nullable somehow.
            for (var e : qe.getFields().entrySet()) {
                e.setValue(ClassnameUtils.ensureWrapper(e.getValue()));
            }

            // Make sure keyFieldName and keyFieldTypes are set.
            List<Map.Entry<String, String>> keyFields =
                    qe.getFields().entrySet().stream().filter(f -> qe.getKeyFields().contains(f.getKey())).collect(Collectors.toList());
            @Nullable String keyFieldName = qe.getKeyFieldName();
            if (keyFieldName != null) {
                @Nullable String foundType = qe.findKeyType();
                if (foundType == null) {
                    throw FieldNameConflictException.forSpecificField("key", keyFieldName);
                }

                qe.getFields().put(keyFieldName, ClassnameUtils.ensureWrapper(foundType));
                qe.getKeyFields().add(keyFieldName);
            } else if (keyFields.size() == 1) {
                qe.setKeyFieldName(keyFields.get(0).getKey());
                qe.setKeyType(keyFields.get(0).getValue());
            }

            // Make sure valFieldName and valFieldTypes are set.
            List<Map.Entry<String, String>> valueFields =
                    qe.getFields().entrySet().stream().filter(f -> !qe.getKeyFields().contains(f.getKey())).collect(Collectors.toList());
            @Nullable String valFieldName = qe.getValueFieldName();
            if (valFieldName != null) {
                @Nullable String foundType = qe.findValueType();
                if (foundType == null) {
                    throw FieldNameConflictException.forSpecificField("value", valFieldName);
                }

                qe.getFields().put(valFieldName, ClassnameUtils.ensureWrapper(foundType));
            } else if (valueFields.size() == 1) {
                qe.setValueFieldName(valueFields.get(0).getKey());
                qe.setValueType(valueFields.get(0).getValue());
            }

            // Mark keyFields as not nullable.
            qe.getNotNullFields().addAll(qe.getKeyFields());
        }

        // Inspect classes that are on the classpath
        List<InspectedField> keyFields = inspectTypeName(qe.getKeyType(), "KEY");
        List<InspectedField> valFields = inspectTypeName(qe.getValueType(), "VALUE");

        // Check duplicated field names, and assign custom field names
        {
            Set<String> fieldNames = new HashSet<>(qe.getFields().size() + keyFields.size() + valFields.size());
            List<Map.Entry<InspectedField, Supplier<String>>> unnamedFields = new ArrayList<>(keyFields.size() + valFields.size());

            Supplier<String> keyFieldNameCandidates = new FieldNameCandidateSupplier(ID_FIELD_NAME_CANDIDATES, n -> "KEY_" + n);
            Supplier<String> valFieldCandidates = new FieldNameCandidateSupplier(VALUE_FIELD_NAME_CANDIDATES, n -> "VAL_" + n);
            Stream<Triple<InspectedField, Supplier<Map.Entry<String, String>>, Supplier<String>>> x = Stream.concat(
                    keyFields.stream().map(f -> Triple.of(f, () -> keyField(qe), keyFieldNameCandidates)),
                    valFields.stream().map(f -> Triple.of(f, () -> valField(qe), valFieldCandidates))
            );

            // Add fields already in the QE
            // Also add the aliases, we don't need collisions on that either.
            Stream.concat(qe.getFields().keySet().stream(), qe.getAliases().values().stream())
                    .map(String::toUpperCase).forEach(fieldNames::add);

            for (Iterator<Triple<InspectedField, Supplier<Map.Entry<String, String>>, Supplier<String>>> it = x.iterator();
                    it.hasNext(); ) {
                Triple<InspectedField, Supplier<Map.Entry<String, String>>, Supplier<String>> entry = it.next();
                InspectedField inspectedField = entry.getLeft();

                if (inspectedField.fieldName != null) {
                    String fieldNameUpperCase = inspectedField.fieldName.toUpperCase();
                    if (!fieldNames.contains(fieldNameUpperCase)) {
                        fieldNames.add(fieldNameUpperCase);
                    } else {
                        // I've seen some weird cases where there was case mismatch between the class attr name and the qe field.
                        // To accept as the same field, both the name (without casing) and the field type must match.
                        Optional<Map.Entry<String, String>> existingEntryForField = qe.getFields().entrySet().stream()
                                .filter(e -> e.getKey().equalsIgnoreCase(inspectedField.fieldName)
                                        && e.getValue().equals(inspectedField.typeName))
                                .findFirst();

                        if (existingEntryForField.isPresent()) {
                            // We will switch our inspected field name to match the casing in the QE and hope for the best.
                            inspectedField.fieldName = existingEntryForField.get().getKey();
                        } else {
                            throw new FieldNameConflictException(inspectedField, fieldNames);
                        }
                    }
                } else if (inspectedField.fieldType == FieldType.PRIMITIVE) {
                    @Nullable var field = entry.getMiddle().get();
                    if (field == null) {
                        unnamedFields.add(Map.entry(inspectedField, entry.getRight()));
                    } else if (inspectedField.typeName.equals(field.getValue())) {
                        inspectedField.fieldName = field.getKey();
                    } else {
                        throw FieldNameConflictException.forSpecificField(inspectedField.fieldName, inspectedField.typeName,
                                field.getValue());
                    }
                } else {
                    unnamedFields.add(Map.entry(inspectedField, entry.getRight()));
                }
            }

            // Assign custom field names
            for (Map.Entry<InspectedField, Supplier<String>> unnamedFieldEntry : unnamedFields) {
                // Get a valid candidate for the field.
                String fieldName = Stream.generate(unnamedFieldEntry.getValue())
                        .filter(not(fieldNames::contains))
                        .findFirst()
                        .get();

                fieldNames.add(fieldName);
                unnamedFieldEntry.getKey().fieldName = fieldName;
            }
        }

        // Empty field lists means that the class for the type is not available on the classpath so it must be a pojo.
        boolean mapsPojo = keyFields.isEmpty() || valFields.isEmpty();

        // Process key fields
        {
            // Set keyFieldName if there is only one key field.
            if (keyFields.size() == 1) {
                qe.setKeyFieldName(keyFields.get(0).fieldName);
                qe.setKeyType(keyFields.get(0).typeName);
            }

            for (InspectedField inspectedField : keyFields) {
                qe.getKeyFields().add(inspectedField.fieldName);

                addToQueryEntity(qe, inspectedField, false);
                mapsPojo = mapsPojo || inspectedField.fieldType == FieldType.POJO_ATTRIBUTE;
            }
        }

        // Process value fields
        {
            if (valFields.size() == 1) {
                qe.setValueFieldName(valFields.get(0).fieldName);
                qe.setValueType(valFields.get(0).typeName);
            }

            for (InspectedField inspectedField : valFields) {
                addToQueryEntity(qe, inspectedField);
                mapsPojo = mapsPojo || inspectedField.fieldType == FieldType.POJO_ATTRIBUTE;
            }
        }

        if (mapsPojo && allowExtraFields) {
            // TODO: GG-40813 Use a default field value instead of nullable.
            qe.getFieldsPrecision().putIfAbsent(EXTRA_FIELDS_COLUMN_NAME, DEFAULT_BINARY_FIELD_LENGTH);
            qe.getFields().put(EXTRA_FIELDS_COLUMN_NAME, byte[].class.getName());
        }

        return qe;
    }

    /**
     * Generate table based on the provided {@link CacheConfiguration}.
     *
     * @param cacheCfg The cache configuration.
     * @return The generate table result.
     * @throws FieldNameConflictException in case of conflicts during the mapping.
     */
    public GenerateTableResult generate(CacheConfiguration<?, ?> cacheCfg) throws FieldNameConflictException {
        String schema = Optional.ofNullable(cacheCfg.getSqlSchema()).orElse(DEFAULT_SCHEMA);
        String tableName = "\"" + cacheCfg.getName() + "\"";
        // TODO: check if tableName needs quoting.

        QueryEntity qryEntity = getOrCreateQueryEntity(cacheCfg);

        int defIdx = 0;
        int pkIdx = 0;
        String[] pkColumnNames = new String[qryEntity.getKeyFields().size()];
        ColumnDefinition[] colDefinitions = new ColumnDefinition[qryEntity.getFields().size()];
        Map<String, String> fieldNameForColumnMappings = new HashMap<>(qryEntity.getFields().size());
        for (Map.Entry<String, String> entry : qryEntity.getFields().entrySet()) {
            String fieldName = entry.getKey();
            Class<?> klass;
            try {
                klass = ClassUtils.getClass(this.clientClassLoader, entry.getValue());
                if (klass.isEnum()) {
                    klass = Enum.class;
                }
            } catch (ClassNotFoundException e) {
                throw FieldNameConflictException.forUnknownType(fieldName, entry.getValue());
            }

            Integer precision = qryEntity.getFieldsPrecision().get(fieldName);
            Integer scale = qryEntity.getFieldsScale().get(fieldName);
            var colType = ColumnType.of(klass)
                    .length(precision)
                    .precision(precision, scale)
                    .nullable(!qryEntity.getNotNullFields().contains(fieldName));

            String dirtyColumnName = qryEntity.getAliases().getOrDefault(fieldName, fieldName);
            String columnName = sanitizeColumnName(dirtyColumnName);
            colDefinitions[defIdx++] = ColumnDefinition.column(columnName, colType);

            if (qryEntity.getKeyFields().contains(fieldName)) {
                pkColumnNames[pkIdx++] = columnName;
            }

            fieldNameForColumnMappings.put(columnName, fieldName);
        }

        var table = TableDefinition.builder(tableName)
                .schema(schema)
                .columns(colDefinitions)
                .primaryKey(pkColumnNames)
                .build();

        // TODO: Test one of these are null;
        @Nullable Map.Entry<String, String> typeHints = Map.entry(qryEntity.getKeyType(), qryEntity.getValueType());
        return new GenerateTableResult(table, fieldNameForColumnMappings, typeHints);
    }

    public TableDefinition generateTableDefinition(CacheConfiguration<?, ?> cacheCfg) throws FieldNameConflictException {
        return generate(cacheCfg).tableDefinition;
    }

    private QueryEntity getOrCreateQueryEntity(CacheConfiguration cacheCfg) throws FieldNameConflictException {
        // TODO: Map the whole object and key instead of the query entities
        QueryEntity qe;

        Map.Entry<Class<?>, Class<?>> typeHints = null;
        try {
            typeHints = this.tableTypeRegistry.typesForTable(cacheCfg.getName());
        } catch (ClassNotFoundException ex) {
            LOGGER.error("Found TableTypeHint for cache but one of the class was not in the Classpath: {}", cacheCfg.getName(), ex);
        }

        if (typeHints != null) {
            LOGGER.warn("Found TableTypeHint for cache: {}:{}", cacheCfg.getName(), typeHints);
            qe = new QueryEntity(typeHints.getKey(), typeHints.getValue());
        } else if (cacheCfg.getQueryEntities().isEmpty()) {
            // This should return a KeyValue Binary Cache
            // TODO: Check if the value type should be null or not.
            qe = new QueryEntity();
            var binaryClsName = byte[].class.getName();
            qe.setKeyType(binaryClsName);
            qe.setValueType(binaryClsName);

            // TODO: Check this default precision for binary caches
            Map<String, Integer> precision = new HashMap<>();
            precision.put("ID", DEFAULT_BINARY_FIELD_LENGTH);
            precision.put("VAL", DEFAULT_BINARY_FIELD_LENGTH);
            qe.setFieldsPrecision(precision);
        } else if (cacheCfg.getQueryEntities().size() == 1) {
            qe = new QueryEntity((QueryEntity) cacheCfg.getQueryEntities().iterator().next());
        } else {
            LOGGER.warn("Unexpected number of entities (Only 0, 1 QueryEntity is support ATM): {}:{}", cacheCfg.getName(),
                    cacheCfg.getQueryEntities().size());
            // TODO: Throw a better checked exception
            throw new RuntimeException("Unsupported number of queryEntities in cache configuration: " + cacheCfg.getQueryEntities().size());
        }

        return populateQueryEntity(qe, allowExtraFields);
    }

    enum FieldType {
        PRIMITIVE,
        ARRAY,
        POJO_ATTRIBUTE
    }

    static class InspectedField {
        @Nullable
        private String fieldName;

        private String typeName;

        private FieldType fieldType;

        private boolean nullable;

        public InspectedField(@Nullable String fieldName, String typeName, FieldType fieldType) {
            this(fieldName, typeName, fieldType, !(fieldType == FieldType.PRIMITIVE || fieldType == FieldType.ARRAY));
        }

        public InspectedField(@Nullable String fieldName, String typeName, FieldType fieldType, boolean nullable) {
            this.fieldName = fieldName;
            this.typeName = typeName;
            this.fieldType = fieldType;
            this.nullable = nullable;
        }

        public String getFieldName() {
            return fieldName;
        }

        public String getTypeName() {
            return typeName;
        }

        public FieldType getFieldType() {
            return fieldType;
        }

        public boolean isNullable() {
            return nullable;
        }

        @Override
        public String toString() {
            return "InspectedField{"
                    + "fieldName='" + fieldName + '\''
                    + ", typeName='" + typeName + '\''
                    + ", fieldType=" + fieldType
                    + ", nullable=" + nullable
                    + '}';
        }
    }

    /** GenerateTableResult. */
    public static class GenerateTableResult {
        private final TableDefinition tableDefinition;

        private final Map<String, String> fieldNameForColumn;

        @Nullable
        private final Map.Entry<String, String> rawTypeHints;

        /**
         * Constructor.
         *
         * @param tableDefinition Table definition.
         * @param fieldNameForColumn Mapping of columns to their corresponding field names.
         * @param rawTypeHints Mapping of type hints by column.
         */
        public GenerateTableResult(TableDefinition tableDefinition, Map<String, String> fieldNameForColumn,
                Map.Entry<String, String> rawTypeHints) {
            this.tableDefinition = tableDefinition;
            this.fieldNameForColumn = fieldNameForColumn;
            this.rawTypeHints = rawTypeHints;
        }

        public TableDefinition tableDefinition() {
            return tableDefinition;
        }

        public Map<String, String> fieldNameForColumnMappings() {
            return fieldNameForColumn;
        }

        @Nullable
        public Map.Entry<String, String> typeHints() {
            return rawTypeHints;
        }
    }

    private static class FieldNameCandidateSupplier implements Supplier<String> {

        private final String[] base;

        private final Function<Integer, String> additional;

        private int idx;

        public FieldNameCandidateSupplier(String[] base, Function<Integer, String> additional) {
            this.base = base;
            this.additional = additional;
            this.idx = 0;
        }

        @Override
        public String get() {
            String ret = (idx < base.length) ? base[idx] : additional.apply(idx - base.length);
            idx++;
            return ret;
        }
    }
}
