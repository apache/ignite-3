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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 *
 */
public abstract class AbstractSchemaSerializer implements SchemaSerialize {
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
     * @param desc
     * @return
     */
    public byte[] serialize(SchemaDescriptor desc) {
        return this.bytes(desc, createByteBuffer(desc));
    }

    /**
     * @param bytes
     * @return
     */
    public SchemaDescriptor deserialize(byte[] bytes) {
        ByteBuffer buf = createByteBuffer(bytes);
        short ver = readVersion(buf);

        return getAssemblerByVersion(ver).value(buf);
    }

    /**
     * @param ver
     * @return
     */
    private SchemaSerialize getAssemblerByVersion(short ver) {
        if (ver == this.version)
            return this;
        else if (this.previous == null)
            throw new IllegalArgumentException();

        return this.previous.getAssemblerByVersion(ver);
    }

    /**
     * @param bytes
     * @return
     */
    protected ByteBuffer createByteBuffer(byte[] bytes) {
        return ByteBuffer.wrap(bytes);
    }

    /**
     * @param desc
     * @return
     */
    protected ByteBuffer createByteBuffer(SchemaDescriptor desc) {
        return ByteBuffer.allocate(size(desc));
    }

    /**
     * @param buf
     * @return
     */
    protected short readVersion(ByteBuffer buf) {
        return buf.getShort();
    }
}
