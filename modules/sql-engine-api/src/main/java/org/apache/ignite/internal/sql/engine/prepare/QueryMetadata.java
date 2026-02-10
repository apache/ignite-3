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
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Query metadata combines {@link ParameterMetadata dynamic parameters metadata} and {@link ColumnMetadata columns metadata}.
 */
public class QueryMetadata {
    @IgniteToStringInclude
    private final List<ColumnMetadata> columns;

    @IgniteToStringInclude
    private final List<ParameterType> parameterTypes;

    public QueryMetadata(ResultSetMetadata resultSetMetadata, ParameterMetadata parameterMetadata) {
        this.columns = resultSetMetadata.columns();
        this.parameterTypes = parameterMetadata.parameterTypes();
    }

    public List<ColumnMetadata> columns() {
        return columns;
    }

    public List<ParameterType> parameterTypes() {
        return parameterTypes;
    }

    @Override
    public String toString() {
        return S.toString(QueryMetadata.class, this);
    }
}
