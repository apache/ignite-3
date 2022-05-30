package org.apache.ignite.internal.client.sql;

import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.sql.ColumnMetadata;

/**
 * Client column metadata.
 */
public class ClientColumnMetadata implements ColumnMetadata {
    /** */
    private final String name;

    /** */
    private final Class<?> valueClass;

    /** */
    private final Object type;

    /** */
    private final boolean nullable;

    /**
     * Constructor.
     *
     * @param unpacker Unpacker.
     */
    public ClientColumnMetadata(ClientMessageUnpacker unpacker) {
        try {
            name = unpacker.unpackString();
            valueClass = Class.forName(unpacker.unpackString());
            type = unpacker.unpackObjectWithType();
            nullable = unpacker.unpackBoolean();
        } catch (ClassNotFoundException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> valueClass() {
        return valueClass;
    }

    /** {@inheritDoc} */
    @Override
    public Object type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullable() {
        return nullable;
    }
}
