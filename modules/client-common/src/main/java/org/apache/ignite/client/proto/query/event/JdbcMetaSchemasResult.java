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
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC tables metadata result.
 */
public class JdbcMetaSchemasResult extends JdbcResponse {
    /** Found schemas. */
    private Collection<String> schemas;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaSchemasResult() {
    }

    /**
     * @param schemas Found schemas.
     */
    public JdbcMetaSchemasResult(Collection<String> schemas) {
        this.schemas = schemas;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (status() != STATUS_SUCCESS)
            return;

        if (schemas == null) {
            packer.packNil();

            return;
        }

        packer.packArrayHeader(schemas.size());

        for (String schema : schemas)
            packer.packString(schema);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (status() != STATUS_SUCCESS)
            return;

        if (unpacker.tryUnpackNil()) {
            schemas = Collections.EMPTY_LIST;

            return;
        }

        int size = unpacker.unpackArrayHeader();

        schemas = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            schemas.add(unpacker.unpackString());
    }

    /**
     * @return Found schemas.
     */
    public Collection<String> schemas() {
        return schemas;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaSchemasResult.class, this);
    }
}
