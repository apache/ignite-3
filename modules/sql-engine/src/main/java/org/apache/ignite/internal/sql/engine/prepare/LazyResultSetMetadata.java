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

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.List;
import java.util.function.Supplier;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Results set metadata holder.
 */
public class LazyResultSetMetadata implements ResultSetMetadata {
    /** Metadata provider. */
    private final Supplier<ResultSetMetadata> columnMetadataProvider;

    /** Columns metadata. */
    private volatile ResultSetMetadata metadata;

    public LazyResultSetMetadata(Supplier<ResultSetMetadata> columnMetadataProvider) {
        this.columnMetadataProvider = columnMetadataProvider;
    }

    /** {@inheritDoc} */
    @Override
    public List<ColumnMetadata> columns() {
        if (metadata == null) {
            metadata = columnMetadataProvider.get();
        }

        return metadata.columns();
    }

    /** {@inheritDoc} */
    @Override
    public int indexOf(String columnName) {
        if (metadata == null) {
            metadata = columnMetadataProvider.get();
        }

        return metadata.indexOf(columnName);
    }
}
