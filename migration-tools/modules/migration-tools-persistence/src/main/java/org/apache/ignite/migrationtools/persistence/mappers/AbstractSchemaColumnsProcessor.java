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

package org.apache.ignite.migrationtools.persistence.mappers;

import static org.apache.ignite.migrationtools.sql.SqlDdlGenerator.EXTRA_FIELDS_COLUMN_NAME;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.function.FailableBiFunction;
import org.apache.commons.lang3.function.FailableFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.migrationtools.persistence.utils.pubsub.BasicProcessor;
import org.apache.ignite.migrationtools.types.converters.TypeConverterFactory;
import org.apache.ignite3.internal.client.table.ClientColumn;
import org.apache.ignite3.internal.client.table.ClientSchema;
import org.apache.ignite3.sql.ColumnType;
import org.apache.ignite3.table.DataStreamerItem;
import org.apache.ignite3.table.Tuple;
import org.apache.ignite3.table.mapper.TypeConverter;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a mapping layer between Ignite 2 Tuples (created from Binary Objects) and a Ignite 3 table schema.
 */
public abstract class AbstractSchemaColumnsProcessor
        extends BasicProcessor<Map.Entry<Object, Object>, DataStreamerItem<Map.Entry<Tuple, Tuple>>>
        implements SchemaColumnsProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSchemaColumnsProcessor.class);

    private final ObjectMapper jsonMapper;

    // Maps: dbColumnName -> tupleColumnName
    private final Map<String, String> fieldNameForColumn;

    private final TypeConverterFactory typeConverterFactory;

    private final List<ColumnMapping> keyColumnMappings;

    private final List<ColumnMapping> valColumnMappings;

    private final Map<Integer, TypeProcessingResult> keyProcessedTypes;

    private final Map<Map.Entry<Integer, Integer>, TypeProcessingResult> valProcessedTypes;

    private final FailableFunction<Object, Tuple, Throwable> keyMapperFunction;

    private final FailableBiFunction<Object, BinaryType, Tuple, Throwable> valMapperFunction;

    private long processedElems = 0;

    private boolean receivedError = false;

    /**
     * Constructor.
     *
     * @param schema Client schema for the mapping table.
     * @param fieldNameForColumn Mapping of fieldnames by column.
     * @param typeConverterFactory A factory for type converters.
     * @param packExtraFields Whether additional field in the records should be packed in the extra fields column.
     */
    public AbstractSchemaColumnsProcessor(
            ClientSchema schema,
            Map<String, String> fieldNameForColumn,
            TypeConverterFactory typeConverterFactory,
            boolean packExtraFields
    ) {
        this.fieldNameForColumn = fieldNameForColumn;
        this.typeConverterFactory = typeConverterFactory;
        this.jsonMapper = createMapper();

        this.keyProcessedTypes = new HashMap<>();
        this.valProcessedTypes = new HashMap<>();

        // Preprocess schema
        {
            // TODO: IGNITE-27629; Optimize; Seems to have a bit of duplicated code, or at least very similar code.
            ClientColumn[] columns = schema.columns();
            int extraFieldsColumnIdx = -1;
            this.keyColumnMappings = new ArrayList<>(columns.length);
            this.valColumnMappings = new ArrayList<>(columns.length);
            for (int i = 0; i < columns.length; i++) {
                ClientColumn col = columns[i];
                String columnNameOrAlias = this.fieldNameForColumn.getOrDefault(col.name(), col.name());
                if (col.key()) {
                    keyColumnMappings.add(new ColumnMapping(col, columnNameOrAlias));
                } else if (!isExtraFieldsColumn(col)) {
                    valColumnMappings.add(new ColumnMapping(col, columnNameOrAlias));
                } else {
                    extraFieldsColumnIdx = i;
                }
            }

            final boolean shouldPackExtraFields = packExtraFields && extraFieldsColumnIdx >= 0;

            // Check if the schema is binary
            if (keyColumnMappings.size() == 1 && keyColumnMappings.get(0).column.type() == ColumnType.BYTE_ARRAY) {
                // Check if we are packing into a binary cache.
                this.keyMapperFunction = keyObject -> packIntoBinary(keyObject, keyColumnMappings.get(0).column.name());
            } else {
                this.keyMapperFunction = keyObject -> {
                    if (keyObject instanceof BinaryObjectImpl) {
                        BinaryObjectImpl boi = (BinaryObjectImpl) keyObject;
                        BinaryType keyRawType = boi.rawType();

                        TypeProcessingResult keyMappingInfo =
                                keyProcessedTypes.computeIfAbsent(keyRawType.typeId(), i -> processBinaryType(keyRawType,
                                        keyColumnMappings, false));

                        return packIntoMany(boi, keyMappingInfo, false);
                    } else if (keyColumnMappings.size() == 1) {
                        String columnName = keyColumnMappings.get(0).column.name();
                        return packIntoSingle(columnName, keyObject);
                    } else {
                        throw RecordMappingException.createUnexpectedRecordTypeError(BinaryObjectImpl.class, keyObject.getClass());
                    }
                };
            }

            if (valColumnMappings.size() == 1 && valColumnMappings.get(0).column.type() == ColumnType.BYTE_ARRAY) {
                // Check if we are packing into a binary cache.
                valMapperFunction = (valObject, any) -> packIntoBinary(valObject, valColumnMappings.get(0).column.name());
            } else {
                valMapperFunction = (valObject, keyBinaryType) -> {
                    if (valObject instanceof BinaryObjectImpl) {
                        BinaryObjectImpl boi = (BinaryObjectImpl) valObject;
                        BinaryType rawType = boi.rawType();

                        // There's no neutral value, so null is as good as any.
                        @Nullable Integer keyTypeId = (keyBinaryType != null) ? keyBinaryType.typeId() : null;
                        Map.Entry<Integer, Integer> processedTypesKey = Pair.of(keyTypeId, rawType.typeId());
                        TypeProcessingResult valMappingInfo = valProcessedTypes.computeIfAbsent(processedTypesKey, e -> {
                            TypeProcessingResult res = processBinaryType(rawType, valColumnMappings, shouldPackExtraFields);
                            if (keyBinaryType != null) {
                                for (String fieldName : keyBinaryType.fieldNames()) {
                                    res.additionalFieldsOnType.remove(fieldName);
                                }
                            }

                            return res;
                        });

                        return packIntoMany(boi, valMappingInfo, shouldPackExtraFields);
                    } else if (valColumnMappings.size() == 1) {
                        String columnName = valColumnMappings.get(0).column.name();
                        return packIntoSingle(columnName, valObject);
                    } else {
                        throw RecordMappingException.createUnexpectedRecordTypeError(BinaryObjectImpl.class, valObject.getClass());
                    }
                };
            }
        }
    }

    private static TypeProcessingResult processBinaryType(BinaryType type, List<ColumnMapping> columnMappings, boolean withExtraFields) {
        // TODO: IGNITE-27629 Optimize stuff with the lowercase names
        var fieldNames = type.fieldNames();
        Map<String, String> lowerCaseFieldNames = new HashMap<>(fieldNames.size());
        for (var fieldName : fieldNames) {
            lowerCaseFieldNames.put(fieldName.toLowerCase(), fieldName);
        }

        List<String> missingColumnsOnType = new ArrayList<>(columnMappings.size());
        List<ColumnMapping> exitingColumns = new ArrayList<>(columnMappings.size());
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(columnMappings.size());
        for (ColumnMapping colRef : columnMappings) {
            // Check if col is null
            String contains = lowerCaseFieldNames.remove(colRef.objectFieldName.toLowerCase());
            if (contains != null) {
                exitingColumns.add(colRef);
                entries.add(Map.entry(colRef.column.name(), entries.size()));
            } else if (!colRef.column.nullable()) {
                missingColumnsOnType.add(colRef.column.name());
            }
        }

        List<String> additionalFieldsOnType = new ArrayList<>(lowerCaseFieldNames.values());

        if (withExtraFields) {
            entries.add(Map.entry(EXTRA_FIELDS_COLUMN_NAME, entries.size()));
        }

        Map.Entry[] columnToIdxEntries = entries.toArray(new Map.Entry[0]);
        Map<String, Integer> columnToIdxMap = Map.ofEntries(columnToIdxEntries);
        return new TypeProcessingResult(exitingColumns, missingColumnsOnType, additionalFieldsOnType, columnToIdxMap);
    }

    public static boolean isExtraFieldsColumn(ClientColumn col) {
        return col.type() == ColumnType.BYTE_ARRAY && EXTRA_FIELDS_COLUMN_NAME.equals(col.name()) && col.nullable();
    }

    /**
     * Creates a JSON Mapper.
     *
     * @return The created JSON Object Mapper.
     */
    public static ObjectMapper createMapper() {
        SimpleModule module = new SimpleModule();
        module.addSerializer(BinaryObject.class, new BinaryObjectSerializer());
        module.addSerializer(WrapperClass.class, new WrapperClassSerializer());
        return new ObjectMapper().registerModule(module);
    }

    @Override
    public void onNext(Map.Entry<Object, Object> item) {
        var key = item.getKey();
        var val = item.getValue();

        @Nullable BinaryType keyRawType = (key instanceof BinaryObjectImpl) ? ((BinaryObjectImpl) key).rawType() : null;
        try {
            Tuple keyTuple = keyMapperFunction.apply(key);
            Tuple valTuple = valMapperFunction.apply(val, keyRawType);

            this.subscriber.onNext(DataStreamerItem.of(Map.entry(keyTuple, valTuple)));
            processedElems++;
        } catch (Throwable e) {
            this.onError(e);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        this.receivedError = true;
        LOGGER.error("Error while mapping cache objects to tuples", throwable);
        super.onError(throwable);
    }

    public boolean hasReceivedError() {
        return receivedError;
    }

    @Override
    public SchemaColumnProcessorStats getStats() {
        return new SchemaColumnProcessorStats(this.processedElems);
    }

    private Tuple packIntoBinary(Object cacheObject, String columnName) throws JsonProcessingException {
        // We can pack everything into the column.
        // We will use JSON for now but it can change in the future.
        byte[] data = (cacheObject instanceof byte[]) ? (byte[]) cacheObject : jsonMapper.writeValueAsBytes(cacheObject);
        return Tuple.create(1).set(columnName, data);
    }

    private Tuple packIntoSingle(String columnName, @Nullable Object val) {
        return Tuple.create(1).set(columnName, val);
    }

    protected Tuple packIntoMany(BinaryObjectImpl cacheObject, TypeProcessingResult typeMappingInfo, boolean packExtraFields)
            throws Exception {
        Tuple ret = typeMappingInfo.createTuple();

        // Optimistically will start empty.
        @Nullable List<String> additionalMissingCols = null;

        for (ColumnMapping columnMapping : typeMappingInfo.availableMappings) {
            Object val = cacheObject.field(columnMapping.objectFieldName);
            if (val != null) {
                // TODO: IGNITE-27629; I'm sure there's a better way to do this. Probably look at the types directly.
                // TODO: IGNITE-27629; Raise an exception on else. We cannot allow Binary Objects after this point.
                // TODO: IGNITE-27629; Create a test with Enum.
                // TODO: IGNITE-27629; Add option to serialize enum to ordinal as well. Enums should be printed as NAME or ORDINAL probably.
                if (val instanceof BinaryObject) {
                    BinaryObject bo = (BinaryObject) val;
                    if (bo.type().isEnum()) {
                        val = bo.enumName();
                    }
                }

                Class<?> columnType = columnMapping.columnType();
                @Nullable TypeConverter converter = typeConverterFactory.converterFor(val.getClass(), columnType);
                if (converter != null) {
                    val = converter.toColumnType(val);
                }
            }

            if (val != null || columnMapping.column.nullable()) {
                ret.set(columnMapping.column.name(), val);
            } else {
                if (additionalMissingCols == null) {
                    additionalMissingCols = new ArrayList<>(typeMappingInfo.missingColumnsOnType.size());
                    additionalMissingCols.addAll(typeMappingInfo.missingColumnsOnType);
                }

                // Found missing at runtime.
                additionalMissingCols.add(columnMapping.column.name());
            }
        }

        Collection<String> additionalFieldOnType;
        if (packExtraFields) {
            // TODO: IGNITE-27629 Optimize with a custom serializer, without wrapping the cache Object in a wrapper class.
            // TODO: IGNITE-27629 Add test for the serialization
            if (!typeMappingInfo.additionalFieldsOnType.isEmpty()) {
                byte[] data = jsonMapper.writeValueAsBytes(new WrapperClass(cacheObject, typeMappingInfo.additionalFieldsOnType));
                ret.set(EXTRA_FIELDS_COLUMN_NAME, data);
            }

            additionalFieldOnType = Collections.emptySet();
        } else {
            additionalFieldOnType = typeMappingInfo.additionalFieldsOnType;
        }

        return postProcessMappedTuple(
                ret, (additionalMissingCols == null) ? typeMappingInfo.missingColumnsOnType : additionalMissingCols, additionalFieldOnType);
    }

    protected abstract Tuple postProcessMappedTuple(Tuple mappedTuple, Collection<String> missingCols, Collection<String> additionalColumns)
            throws RecordAndTableSchemaMismatchException;

    private static class BinaryObjectSerializer extends StdSerializer<BinaryObject> {
        protected BinaryObjectSerializer() {
            super(BinaryObject.class);
        }

        @Override
        public void serialize(BinaryObject object, JsonGenerator generator, SerializerProvider provider) throws IOException {
            BinaryType type = object.type();

            if (type.isEnum()) {
                generator.writeString(object.enumName());
            } else {
                generator.writeStartObject();

                for (String fieldName : type.fieldNames()) {
                    var val = object.field(fieldName);
                    generator.writeObjectField(fieldName, val);
                }

                generator.writeEndObject();
            }
        }
    }

    /** WrapperClass. */
    public static class WrapperClass {
        final BinaryObjectImpl cacheObject;

        final List<String> fields;

        public WrapperClass(BinaryObjectImpl cacheObject, List<String> fields) {
            this.cacheObject = cacheObject;
            this.fields = fields;
        }
    }

    private static class WrapperClassSerializer extends StdSerializer<WrapperClass> {
        protected WrapperClassSerializer() {
            super(WrapperClass.class);
        }

        @Override
        public void serialize(WrapperClass object, JsonGenerator generator, SerializerProvider provider) throws IOException {
            generator.writeStartObject();

            for (String fieldName : object.fields) {
                generator.writeObjectField(fieldName, object.cacheObject.field(fieldName));
            }

            generator.writeEndObject();
        }
    }

    private static class ColumnMapping {
        final ClientColumn column;

        final String objectFieldName;

        public ColumnMapping(ClientColumn column, String objectFieldName) {
            this.column = column;
            this.objectFieldName = objectFieldName;
        }

        public Class<?> columnType() {
            return column.type().javaClass();
        }
    }

    private static class TypeProcessingResult {
        final List<ColumnMapping> availableMappings;

        final Map<String, Integer> nameToIdMap;

        final List<String> missingColumnsOnType;

        final List<String> additionalFieldsOnType;

        public TypeProcessingResult(List<ColumnMapping> availableMappings, List<String> missingColumnsOnType,
                List<String> additionalFieldsOnType,
                Map<String, Integer> nameToIdMap) {
            this.availableMappings = availableMappings;
            this.missingColumnsOnType = missingColumnsOnType;
            this.additionalFieldsOnType = additionalFieldsOnType;
            this.nameToIdMap = nameToIdMap;
        }

        CustomTupleImpl createTuple() {
            return new CustomTupleImpl(nameToIdMap);
        }
    }

    /** RecordMappingException. */
    public static class RecordMappingException extends Exception {
        public RecordMappingException(String message) {
            super(message);
        }

        public static RecordMappingException createUnexpectedRecordTypeError(Class<?> expected, Class<?> found) {
            return new RecordMappingException(
                    "Unexpected record type: Expected '" + expected.getName() + "' found '" + found.getName() + "'");
        }
    }
}
