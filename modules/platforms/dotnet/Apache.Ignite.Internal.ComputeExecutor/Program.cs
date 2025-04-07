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

using Apache.Ignite;

const string envServerAddr = "IGNITE_COMPUTE_EXECUTOR_SERVER_ADDRESS";
string? serverAddr = Environment.GetEnvironmentVariable(envServerAddr);

if (string.IsNullOrWhiteSpace(serverAddr))
{
    throw new InvalidOperationException($"Environment variable {envServerAddr} is not set.");
}

var clientCfg = new IgniteClientConfiguration(serverAddr)
{
    RetryPolicy = RetryNonePolicy.Instance // No reconnect.
};

using var client = await IgniteClient.StartAsync(clientCfg).ConfigureAwait(false);

// Sleep forever. Host process will terminate us when the executor is stopped.
await Task.Delay(Timeout.Infinite).ConfigureAwait(false);
