package org.apache.ignite.internal.sql.engine.exec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.sql.engine.exec.row.BaseTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.NullTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.RowType;
import org.apache.ignite.internal.sql.engine.exec.row.TypeSpec;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.jetbrains.annotations.Nullable;

public class SqlRowSchemaConverterUtils {

    public static @Nullable Object readRow(RowSchema schema, int schemaIndex, InternalTuple row, int index) {
        TypeSpec type = schema.fields().get(schemaIndex);

        if (type instanceof NullTypeSpec) {
            // TODO
            assert row.hasNullValue(index);

            return null;
        }

        NativeType nativeType = toNativeType(type);

        switch (nativeType.spec()) {
            case BOOLEAN: return row.booleanValueBoxed(index);
            case INT8: return row.byteValueBoxed(index);
            case INT16: return row.shortValueBoxed(index);
            case INT32: return row.intValueBoxed(index);
            case INT64: return row.longValueBoxed(index);
            case FLOAT: return row.floatValueBoxed(index);
            case DOUBLE: return row.doubleValueBoxed(index);
            case DECIMAL: return row.decimalValue(index, ((DecimalNativeType) nativeType).scale());
            case UUID: return row.uuidValue(index);
            case STRING: return row.stringValue(index);
            case BYTES: return row.bytesValue(index);
            case BITMASK: return row.bitmaskValue(index);
            case NUMBER: return row.numberValue(index);
            case DATE: return row.dateValue(index);
            case TIME: return row.timeValue(index);
            case DATETIME: return row.dateTimeValue(index);
            case TIMESTAMP: return row.timestampValue(index);
            default: throw new InvalidTypeException("Unknown element type: " + nativeType);
        }
    }

    public static BinaryTupleBuilder appendRow(BinaryTupleBuilder builder, RowSchema schema, int index, Object value) {
        TypeSpec type = schema.fields().get(index);

        if (value == null) {
            return builder.appendNull();
        }

        NativeType nativeType = toNativeType(type);

        value = TypeUtils.fromInternal(value, NativeTypeSpec.toClass(nativeType.spec(), type.isNullable()));

        assert value != null : nativeType;

        switch (nativeType.spec()) {
            case BOOLEAN:
                return builder.appendBoolean((boolean) value);
            case INT8:
                return builder.appendByte((byte) value);
            case INT16:
                return builder.appendShort((short) value);
            case INT32:
                return builder.appendInt((int) value);
            case INT64:
                return builder.appendLong((long) value);
            case FLOAT:
                return builder.appendFloat((float) value);
            case DOUBLE:
                return builder.appendDouble((double) value);
            case NUMBER:
                return builder.appendNumberNotNull((BigInteger) value);
            case DECIMAL:
                return builder.appendDecimalNotNull((BigDecimal) value, ((DecimalNativeType) nativeType).scale());
            case UUID:
                return builder.appendUuidNotNull((UUID) value);
            case BYTES:
                return builder.appendBytesNotNull((byte[]) value);
            case STRING:
                return builder.appendStringNotNull((String) value);
            case BITMASK:
                return builder.appendBitmaskNotNull((BitSet) value);
            case DATE:
                return builder.appendDateNotNull((LocalDate) value);
            case TIME:
                return builder.appendTimeNotNull((LocalTime) value);
            case DATETIME:
                return builder.appendDateTimeNotNull((LocalDateTime) value);
            case TIMESTAMP:
                return builder.appendTimestampNotNull((Instant) value);
            default:
                throw new UnsupportedOperationException("Unknown type " + nativeType);
        }
    }

    private static NativeType toNativeType(TypeSpec type) {
        if (type instanceof RowType) {
            // todo
            return toNativeType(((RowType) type).fields().get(0));
        }

        if (type instanceof BaseTypeSpec) {
            return ((BaseTypeSpec) type).nativeType();
        }

        // todo appendNull()
        assert !(type instanceof NullTypeSpec) : type;

        throw new UnsupportedOperationException("Not implemented for type " + type.getClass());
    }
}
