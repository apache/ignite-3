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

namespace Apache.Ignite.Internal.Table;

using System.Threading.Tasks;

/// <summary>
/// Data streamer provider.
/// </summary>
/// <typeparam name="T">Element type.</typeparam>
/// <typeparam name="TBatch">Batch type.</typeparam>
internal interface IDataStreamerProvider<in T, TBatch>
    where TBatch : IDataStreamerBatch
{
    /// <summary>
    /// Adds an item to the batch.
    /// </summary>
    /// <param name="item">Item.</param>
    /// <param name="schema">Schema.</param>
    /// <returns>Resulting batch.</returns>
    TBatch Add(T item, Schema schema);

    /// <summary>
    /// Sends the batch to the cluster.
    /// </summary>
    /// <param name="batch">Batch.</param>
    /// <param name="retryPolicy">Retry policy.</param>
    /// <param name="schema">Schema.</param>
    /// <returns>Task.</returns>
    Task FlushAsync(TBatch batch, IRetryPolicy retryPolicy, Schema schema);
}
