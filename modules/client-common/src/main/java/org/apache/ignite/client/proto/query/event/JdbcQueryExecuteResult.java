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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC query execute result.
 */
public class JdbcQueryExecuteResult extends JdbcResponse {
    /** Query result rows. */
    private List<JdbcQuerySingleResult> results;

    /**
     * Constructor. For deserialization purposes only.
     */
    public JdbcQueryExecuteResult() {
    }

    /**
     * Constructor.
     */
    public JdbcQueryExecuteResult(int status, @Nullable String err) {
        super(status, err);
    }

    /**
     * @param results Results.
     */
    public JdbcQueryExecuteResult(List<JdbcQuerySingleResult> results) {
        super();

        this.results = results;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(ClientMessagePacker packer) throws IOException {
        super.writeBinary(packer);

        if (status() != STATUS_SUCCESS)
            return;

        assert results != null;

        packer.packInt(results.size());

        if (results.isEmpty())
            return;

        for (JdbcQuerySingleResult result : results)
            result.writeBinary(packer);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(ClientMessageUnpacker unpacker) throws IOException {
        super.readBinary(unpacker);

        if (status() != STATUS_SUCCESS)
            return;

        int size = unpacker.unpackInt();

        if (size == 0) {
            results = Collections.emptyList();

            return;
        }

        results = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            var res = new JdbcQuerySingleResult();
            res.readBinary(unpacker);

            results.add(res);
        }
    }

    /**
     * @return Query result rows.
     */
    public List<JdbcQuerySingleResult> results() {
        return results;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcQueryExecuteResult.class, this);
    }
}
