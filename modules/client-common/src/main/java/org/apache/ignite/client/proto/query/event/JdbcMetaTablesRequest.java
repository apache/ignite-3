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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC tables metadata request.
 */
public class JdbcMetaTablesRequest implements JdbcClientMessage {
    /** Schema search pattern. */
    private String schemaName;

    /** Table search pattern. */
    private String tblName;

    /** Table types. */
    private String[] tblTypes;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaTablesRequest() {
    }

    /**
     * @param schemaName Schema search pattern.
     * @param tblName Table search pattern.
     * @param tblTypes Table types.
     */
    public JdbcMetaTablesRequest(String schemaName, String tblName, String[] tblTypes) {
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.tblTypes = tblTypes;
    }

    /**
     * @return Schema search pattern.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table search pattern.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Table types.
     */
    public String[] tableTypes() {
        return tblTypes;
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

        if (tblTypes == null) {
            packer.packNil();

            return;
        }
        packer.packArrayHeader(tblTypes.length);

        for (String type : tblTypes)
            packer.packString(type);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        if (!unpacker.tryUnpackNil())
            schemaName = unpacker.unpackString();

        if (!unpacker.tryUnpackNil())
            tblName = unpacker.unpackString();

        if (unpacker.tryUnpackNil())
            return;

        int size = unpacker.unpackArrayHeader();

        List<String> types = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            types.add(unpacker.unpackString());

        tblTypes = types.toArray(new String[0]);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaTablesRequest.class, this);
    }
}