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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

/**
 * Listen storage changes and pass the updates to the {@link TableImpl}.
 */
public class TableStorageRowListener implements StorageRowListener {
    private StorageRowListener delegate;

    /** {@inheritDoc} */
    @Override
    public void onUpdate(@Nullable BinaryRow oldRow, BinaryRow newRow) {
        if (delegate != null) {
            delegate.onUpdate(oldRow, newRow);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onRemove(BinaryRow row) {
        if (delegate != null) {
            delegate.onRemove(row);
        }
    }

    /**
     * Listen storage changes and pass the updates to the {@link TableImpl}.
     */
    public void listen(StorageRowListener lsnr) {
        delegate = lsnr;
    }
}
