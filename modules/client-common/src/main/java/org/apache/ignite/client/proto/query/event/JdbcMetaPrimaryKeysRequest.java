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

package org.apache.ignite.client.proto.query.event;

import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC get primary keys metadata request.
 */
public class JdbcMetaPrimaryKeysRequest implements JdbcClientMessage {
    /** Schema name pattern. */
    private String schemaName;

    /** Table name pattern. */
    private String tblName;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaPrimaryKeysRequest() {
    }

    /**
     * @param schemaName Cache name.
     * @param tblName Table name.
     */
    public JdbcMetaPrimaryKeysRequest(String schemaName, String tblName) {
        this.schemaName = schemaName;
        this.tblName = tblName;
    }

    /**
     * @return Schema name pattern.
     */
    @Nullable public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name pattern.
     */
    public String tableName() {
        return tblName;
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        if (!unpacker.tryUnpackNil())
            schemaName = unpacker.unpackString();

        if (!unpacker.tryUnpackNil())
            tblName = unpacker.unpackString();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        if (schemaName == null)
            packer.packNil();
        else
            packer.packString(schemaName);

        if (tblName == null)
            packer.packNil();
        else
            packer.packString(tblName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaPrimaryKeysRequest.class, this);
    }
}