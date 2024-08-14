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

namespace Apache.Ignite.Compute;

using System;
using System.Buffers;
using System.Collections.Generic;
using Marshalling;

/// <summary>
/// Compute job descriptor.
/// </summary>
/// <param name="JobClassName">Java class name of the job to execute.</param>
/// <param name="DeploymentUnits">Deployment units.</param>
/// <param name="Options">Options.</param>
/// <param name="ArgMarshaller">Argument marshaller (serializer).</param>
/// <param name="ResultMarshaller">Result marshaller (deserializer).</param>
/// <typeparam name="TArg">Argument type.</typeparam>
/// <typeparam name="TResult">Result type.</typeparam>
public sealed record JobDescriptor<TArg, TResult>(
    string JobClassName,
    IEnumerable<DeploymentUnit>? DeploymentUnits = null,
    JobExecutionOptions? Options = null,
    IMarshaller<TArg>? ArgMarshaller = null,
    IMarshaller<TResult>? ResultMarshaller = null);
