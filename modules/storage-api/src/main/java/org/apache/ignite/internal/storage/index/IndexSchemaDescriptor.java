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

package org.apache.ignite.internal.storage.index;

import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;

/**
 * Index column collation: direction and NULLs order that a column's value is ordered in.
 */
public class IndexSchemaDescriptor extends SchemaDescriptor {
    private static final Column[] EMPTY_COLUMNS_ARRAY = new Column[0];

    public IndexSchemaDescriptor(Column[] cols) {
        super(0, cols, EMPTY_COLUMNS_ARRAY);
    }

    /**
     * Returns index columns.
     */
    public Column[] columns() {
        return keyColumns().columns();
    }
}
