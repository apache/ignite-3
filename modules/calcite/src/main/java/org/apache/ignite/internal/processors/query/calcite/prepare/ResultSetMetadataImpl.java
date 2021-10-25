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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.ResultFieldMetadata;
import org.apache.ignite.internal.processors.query.calcite.ResultSetMetadata;

/**
 * Results set metadata holder.
 */
public class ResultSetMetadataImpl implements ResultSetMetadataInternal {
    /** Fields metadata. */
    private final List<ResultFieldMetadata> fields;

    /** Internal row type. */
    private final RelDataType rowType;

    public ResultSetMetadataImpl(
        RelDataType rowType,
        List<ResultFieldMetadata> fields
    ) {
        this.rowType = rowType;
        this.fields = fields;
    }

    /** {@inheritDoc} */
    @Override public List<ResultFieldMetadata> fields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override public RelDataType rowType() {
        return rowType;
    }
}
