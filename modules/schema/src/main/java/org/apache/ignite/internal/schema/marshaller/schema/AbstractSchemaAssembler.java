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
public abstract class AbstractSchemaAssembler implements SchemaAssembler {
    /** Version. */
    protected final byte version;

    /** Previous assembler version. */
    protected final AbstractSchemaAssembler previousVersion;

    /**
     * @param ver Assembler version.
     * @param previousVer Previous assembler version.
     */
    protected AbstractSchemaAssembler(byte ver, AbstractSchemaAssembler previousVer) {
        this.version = ver;
        this.previousVersion = previousVer;
    }

    /**
     * @param ver Assembler version.
     */
    protected AbstractSchemaAssembler(byte ver) {
        this.version = ver;
        this.previousVersion = null;
    }

    /**
     * @return Assembler version;
     */
    public byte getVersion() {
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
        byte ver = readVersion(buf);

        return getAssemblerByVersion(ver).value(buf);
    }

    /**
     * @param ver
     * @return
     */
    private SchemaAssembler getAssemblerByVersion(byte ver) {
        if (ver == this.version)
            return this;
        else if (this.previousVersion == null)
            throw new IllegalArgumentException();

        return this.previousVersion.getAssemblerByVersion(ver);
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
    protected byte readVersion(ByteBuffer buf) {
        return buf.get();
    }
}
