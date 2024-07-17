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

namespace Apache.Ignite.Internal;

using System;

/// <summary>
/// Retry policy context.
/// </summary>
/// <param name="Configuration">Client configuration.</param>
/// <param name="Operation">Operation.</param>
/// <param name="Iteration">Current iteration.</param>
/// <param name="Exception">Exception that caused current retry iteration.</param>
internal sealed record RetryPolicyContext(
    IgniteClientConfiguration Configuration,
    ClientOperationType Operation,
    int Iteration,
    Exception Exception) : IRetryPolicyContext;
