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

package org.apache.ignite.internal.jdbc.proto.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC schemas result.
 */
public class JdbcMetaSchemasResult extends Response {
    /** Found schemas. */
    private Collection<String> schemas;

    /**
     * Default constructor is used for deserialization.
     */
    public JdbcMetaSchemasResult() {
    }

    /**
     * Constructor.
     *
     * @param schemas Found schemas.
     */
    public JdbcMetaSchemasResult(Collection<String> schemas) {
        Objects.requireNonNull(schemas);

        this.schemas = schemas;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!success()) {
            return;
        }

        packer.packInt(schemas.size());

        for (String schema : schemas) {
            packer.packString(schema);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!success()) {
            return;
        }

        int size = unpacker.unpackInt();

        schemas = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            schemas.add(unpacker.unpackString());
        }
    }

    /**
     * Gets found table schemas.
     *
     * @return Found schemas.
     */
    public Collection<String> schemas() {
        return schemas;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(JdbcMetaSchemasResult.class, this);
    }
}
