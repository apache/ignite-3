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

package org.apache.ignite.internal.table.distributed.index;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.ColumnsExtractor;

/**
 * Convenient wrapper which glues together a function which actually converts one row to another, and a version of the schema the function
 * was build upon.
 */
class VersionedConverter implements ColumnsExtractor {
    private final int version;

    private final ColumnsExtractor delegate;

    VersionedConverter(int version, ColumnsExtractor delegate) {
        this.version = version;
        this.delegate = delegate;
    }

    @Override
    public BinaryTuple extractColumns(BinaryRow row) {
        return delegate.extractColumns(row);
    }

    int version() {
        return version;
    }
}
