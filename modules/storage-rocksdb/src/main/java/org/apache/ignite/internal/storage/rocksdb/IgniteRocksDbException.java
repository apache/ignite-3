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

package org.apache.ignite.internal.storage.rocksdb;

import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Storage.STORAGE_CORRUPTED_ERR;

import org.apache.ignite.internal.storage.StorageException;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.rocksdb.Status.Code;

/**
 * Exception that gets thrown if an error occurs in the underlying RocksDB storage.
 */
public class IgniteRocksDbException extends StorageException {
    public IgniteRocksDbException(RocksDBException cause) {
        super(parseErrorCode(cause), cause);
    }

    public IgniteRocksDbException(String message, RocksDBException cause) {
        super(parseErrorCode(cause), message, cause);
    }

    public IgniteRocksDbException(String message, RocksDBException cause, Object... params) {
        super(parseErrorCode(cause), message, cause, params);
    }

    private static int parseErrorCode(RocksDBException ex) {
        Status status = ex.getStatus();

        if (status != null && status.getCode() == Code.Corruption) {
            return STORAGE_CORRUPTED_ERR;
        } else {
            return INTERNAL_ERR;
        }
    }
}
