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

package org.apache.ignite.internal.jdbc.proto.event;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.tostring.S;

/**
 * JDBC query execute result.
 */
public class QueryExecuteResult extends Response {
    /** Query result rows. */
    private List<QuerySingleResult> results;

    /**
     * Constructor. For deserialization purposes only.
     */
    public QueryExecuteResult() {
    }

    /**
     * Constructor.
     *
     * @param status Status code.
     * @param err    Error message.
     */
    public QueryExecuteResult(int status, String err) {
        super(status, err);
    }

    /**
     * Constructor.
     *
     * @param results Results.
     */
    public QueryExecuteResult(List<QuerySingleResult> results) {
        super();

        Objects.requireNonNull(results);

        this.results = results;

        this.hasResults = true;
    }

    /** {@inheritDoc} */
    @Override
    public void writeBinary(ClientMessagePacker packer) {
        super.writeBinary(packer);

        if (!hasResults) {
            return;
        }

        packer.packArrayHeader(results.size());

        for (QuerySingleResult result : results) {
            result.writeBinary(packer);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readBinary(ClientMessageUnpacker unpacker) {
        super.readBinary(unpacker);

        if (!hasResults) {
            return;
        }

        int size = unpacker.unpackArrayHeader();

        if (size == 0) {
            results = Collections.emptyList();

            return;
        }

        results = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            var res = new QuerySingleResult();
            res.readBinary(unpacker);

            results.add(res);
        }
    }

    /**
     * Get the query results.
     *
     * @return Query result rows.
     */
    public List<QuerySingleResult> results() {
        return results;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(QueryExecuteResult.class, this);
    }
}
