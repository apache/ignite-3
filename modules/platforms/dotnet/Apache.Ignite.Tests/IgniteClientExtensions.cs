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

namespace Apache.Ignite.Tests;

/// <summary>
/// Extensions for <see cref="IIgniteClient"/>.
/// </summary>
public static class IgniteClientExtensions
{
    /// <summary>
    /// Waits for the specified number of connections to be established. Throws an exception if the condition is not reached.
    /// </summary>
    /// <param name="client">Client.</param>
    /// <param name="count">Connection count.</param>
    /// <param name="timeoutMs">Timeout.</param>
    public static void WaitForConnections(this IIgniteClient client, int count, int timeoutMs = 15_000) =>
        TestUtils.WaitForCondition(
            condition: () => client.GetConnections().Count == count,
            timeoutMs: timeoutMs,
            messageFactory: () => $"Connection count: expected = {count}, actual = {client.GetConnections().Count}");
}
