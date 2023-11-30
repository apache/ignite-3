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

package org.apache.ignite.internal.sql.engine.message;

import java.util.Map;
import org.apache.ignite.internal.sql.engine.QueryCatalogVersions;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.network.annotations.Marshallable;
import org.apache.ignite.network.annotations.Transferable;

/**
 * QueryStartRequest interface.
 */
@Transferable(value = SqlQueryMessageGroup.QUERY_START_REQUEST)
public interface QueryStartRequest extends ExecutionContextAwareMessage {
    /**
     * Get fragment description.
     */
    @Marshallable
    FragmentDescription fragmentDescription();

    /**
     * Get fragment plan.
     */
    String root();

    /**
     * Get query parameters.
     */
    @Marshallable
    Object[] parameters();

    /**
     * Transaction id.
     */
    @Marshallable
    TxAttributes txAttributes();

    /**
     * Returns base schema version corresponding to the request. This schema version may be overriden
     * for individual tables via {@link #schemaVersionTableOverrides()}.
     *
     * @see #schemaVersionTableOverrides()
     */
    int baseSchemaVersion();

    /**
     * Returns overrides of schema versions for this request.
     *
     * @return Map of overrides, keys are table IDs, values are catalog versions.
     */
    Map<Integer, Integer> schemaVersionTableOverrides();

    /**
     * Returns information about catalog versions fixed for this query.
     */
    default QueryCatalogVersions schemaVersions() {
        return new QueryCatalogVersions(baseSchemaVersion(), schemaVersionTableOverrides());
    }
}
