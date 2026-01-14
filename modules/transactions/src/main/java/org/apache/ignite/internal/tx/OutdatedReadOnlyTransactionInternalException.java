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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_READ_ONLY_TOO_OLD_ERR;

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * Exception thrown when read-only transaction appears to be outdated due to its read timestamp that is less than low watermark.
 */
public class OutdatedReadOnlyTransactionInternalException extends IgniteInternalException {
    private static final long serialVersionUID = 3136265772703827255L;

    /**
     * Constructor.
     *
     * @param txId Transaction id.
     */
    public OutdatedReadOnlyTransactionInternalException(UUID txId) {
        super(TX_READ_ONLY_TOO_OLD_ERR, "Outdated read-only transaction [txId" + txId + "].");
    }
}
