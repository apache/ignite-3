/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage;

import java.util.UUID;
import org.apache.ignite.lang.IgniteException;

/**
 * Exception class that describes the situation when two independent transactions attempt to write values for the same key.
 */
public class TxIdMismatchException extends IgniteException {
    private final UUID transactionId;

    public TxIdMismatchException(UUID transactionId) {
        this.transactionId = transactionId;
    }

    public UUID transactionId() {
        return transactionId;
    }
}
