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

if (args.Length != 2)
{
    Console.WriteLine("2 arguments required: <address>, <executorId>");
    return 1;
}

var addr = args[0];
var executorId = args[1];

// TODO: How to pass executorId?
await IgniteClient.StartAsync(new IgniteClientConfiguration()).ConfigureAwait(false);

// Sleep forever. Host process will terminate us when the executor is stopped.
await Task.Delay(Timeout.Infinite).ConfigureAwait(false);
return 0;
