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

namespace Apache.Ignite.Transactions;

/// <summary>
/// Ignite transaction options.
/// </summary>
/// <param name="ReadOnly">
/// Whether to start a read-only transaction.
/// Read-only transactions provide a snapshot view of data at a certain point in time.
/// They are lock-free and perform better than normal transactions, but do not permit data modifications.
/// </param>
/// <param name="TimeoutMillis">
/// Transaction timeout. 0 means 'use default timeout'.
/// For RO transactions, the default timeout is data availability time configured via ignite.gc.lowWatermark.dataAvailabilityTimeMillis
/// configuration setting.
/// For RW transactions, timeouts are not supported yet. TODO: IGNITE-15936.
/// </param>
public readonly record struct TransactionOptions(bool ReadOnly, long TimeoutMillis = 0);
