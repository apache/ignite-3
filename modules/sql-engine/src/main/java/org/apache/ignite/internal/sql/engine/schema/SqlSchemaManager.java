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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.concurrent.CompletableFuture;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Sql schemas operations interface.
 */
public interface SqlSchemaManager {
    /**
     * Returns root schema derived from catalog of the given version.
     */
    SchemaPlus schema(int catalogVersion);

    /**
     * Returns root schema derived from catalog of version which was considered active at the given timestamp.
     */
    SchemaPlus schema(long timestamp);

    /**
     * Returns table by given id, which version correspond to the one from catalog of given version.
     *
     * @param catalogVersion Version of the catalog.
     * @param tableId An identifier of a table of interest.
     * @return A table.
     */
    IgniteTable table(int catalogVersion, int tableId);

    /**
     * Returns a future to wait for given catalog version readiness.
     *
     * @param catalogVersion version of the catalog to wait.
     */
    CompletableFuture<Void> schemaReadyFuture(int catalogVersion);
}
