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

package org.apache.ignite.internal.schema.marshaller.schema;

import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 * Schema serializer.
 */
public abstract class AbstractSchemaSerializer implements SchemaSerializer {
    /** Schema serializer version. */
    protected final short version;

    /** Previous version serializer. */
    protected final AbstractSchemaSerializer previous;

    /**
     * @param ver Assembler version.
     * @param previous Previous version serializer.
     */
    protected AbstractSchemaSerializer(byte ver, AbstractSchemaSerializer previous) {
        this.version = ver;
        this.previous = previous;
    }

    /**
     * @param ver Assembler version.
     */
    protected AbstractSchemaSerializer(short ver) {
        this.version = ver;
        this.previous = null;
    }

    /**
     * @return Assembler version;
     */
    public short getVersion() {
        return version;
    }

    /**
     * Serialize SchemaDescriptor object to byte array.
     *
     * @param desc SchemaDescriptor object.
     * @return SchemaDescriptor byte array representation.
     */
    public byte[] serialize(SchemaDescriptor desc) {
        return this.bytes(desc, createByteBuffer(desc)).array();
    }

    /**
     * Deserialize byte array to SchemaDescriptor object.
     *
     * @param bytes SchemaDescriptor byte array representation.
     * @return SchemaDescriptor object.
     */
    public SchemaDescriptor deserialize(byte[] bytes) {
        ExtendedByteBuffer buf = createByteBuffer(bytes);

        short ver = readVersion(buf);

        return getAssemblerByVersion(ver).value(buf);
    }

    /**
     * Gets schema serializer by version.
     *
     * @param ver SchemaSerializer target version.
     * @return SchemaSerializer object.
     * @throws IllegalArgumentException If SchemaSerializer with right version is not found.
     */
    private SchemaSerializer getAssemblerByVersion(short ver) {
        if (ver == this.version)
            return this;
        else if (this.previous == null)
            throw new IllegalArgumentException();

        return this.previous.getAssemblerByVersion(ver);
    }

    /**
     * Calculate size in bytes of SchemaDescriptor object.
     *
     * @param desc SchemaDescriptor object.
     * @return Size in bytes.
     */
    private int size(SchemaDescriptor desc) {
        return bytes(desc, ExtendedByteBufferFactory.calcSize()).size();
    }

    /**
     * Wraps a byte array into a buffer.
     *
     * @param bytes Byte array.
     * @return ByteBuffer byte buffer.
     */
    protected ExtendedByteBuffer createByteBuffer(byte[] bytes) {
        return ExtendedByteBufferFactory.wrap(bytes);
    }

    /**
     * Calculates the required size from SchemaDescriptor object and allocates a new ByteBuffer with the given size.
     *
     * @param desc SchemaDescriptor object.
     * @return ByteBuffer object.
     */
    protected ExtendedByteBuffer createByteBuffer(SchemaDescriptor desc) {
        return ExtendedByteBufferFactory.allocate(size(desc));
    }

    /**
     * Reads SchemaSerializer version from byte buffer.
     *
     * @param buf ByteBuffer object.
     * @return SchemaSerializer version.
     */
    protected short readVersion(ExtendedByteBuffer buf) {
        return buf.getShort();
    }
}
