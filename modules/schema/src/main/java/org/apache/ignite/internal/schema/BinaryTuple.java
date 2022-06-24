package org.apache.ignite.internal.schema;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.schema.row.InternalTuple;

/**
 * Utility for access to binary tuple elements as typed values and with schema knowledge that allows to read
 * elements as objects.
 */
public class BinaryTuple extends BinaryTupleReader implements InternalTuple {
    /** Tuple schema. */
    private final BinaryTupleSchema schema;

    /**
     * Constructor.
     *
     * @param schema Tuple schema.
     * @param bytes Binary tuple.
     */
    public BinaryTuple(BinaryTupleSchema schema, byte[] bytes) {
        super(schema.elementCount(), bytes);
        this.schema = schema;
    }

    /**
     * Constructor.
     *
     * @param schema Tuple schema.
     * @param buffer Buffer with a binary tuple.
     */
    public BinaryTuple(BinaryTupleSchema schema, ByteBuffer buffer) {
        super(schema.elementCount(), buffer);
        this.schema = schema;
    }

    /** {@inheritDoc} */
    @Override
    public int count() {
        return elementCount();
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal decimalValue(int index) {
        return decimalValue(index, schema.element(index).decimalScale);
    }

    /**
     * Reads value for specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Object value(int index) {
        return schema.element(index).typeSpec.objectValue(this, index);
    }
}
