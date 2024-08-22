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

package org.apache.ignite.internal.tx.configuration;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/**
 * Configuration schema for transactions.
 */
@Config
public class TransactionConfigurationSchema {
    /** Default checking transaction interval. */
    public static final long DEFAULT_ABANDONED_CHECK_TS = 5_000;

    /** Checking transaction interval. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public final long abandonedCheckTs = DEFAULT_ABANDONED_CHECK_TS;

    /** Timeout for implicit transactions. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public final long implicitTransactionTimeout = 3_000;

    /** A transaction tries to take lock several times until it throws an exception {@lonk org.apache.ignite.tx.TransactionException}. */
    @Range(min = 0)
    @Value(hasDefault = true)
    public final int attemptsObtainLock = 3;

    /** Transaction resource time to live (ms), the minimum lifetime of a transaction state. */
    @Value(hasDefault = true)
    @Range(min = 0)
    public long txnResourceTtl = TimeUnit.SECONDS.toMillis(30);

    /** Transaction system remote call timeout. RPC timeout for operations like cleanup and write intent resolution. */
    @Value(hasDefault = true)
    @Range(min = 1000)
    public long rpcTimeout = TimeUnit.SECONDS.toMillis(60);
}
