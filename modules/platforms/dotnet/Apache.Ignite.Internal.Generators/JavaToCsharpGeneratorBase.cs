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

namespace Apache.Ignite.Internal.Generators;

using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;

/// <summary>
/// Base class for Java -> C# source generators.
/// <para />
/// <c>IIncrementalGenerator</c> is not used because it makes directory detection too hard.
/// <see cref="GeneratorInitializationContext.RegisterForPostInitialization"/> is not used for the same reason.
/// </summary>
public abstract class JavaToCsharpGeneratorBase : ISourceGenerator
{
    private List<(string Name, string Code)>? _generatedCode;

    /// <inheritdoc/>
    public void Initialize(GeneratorInitializationContext context)
    {
        // No-op.
    }

    /// <inheritdoc/>
    public void Execute(GeneratorExecutionContext context)
    {
        // Execute the generator only once during full build.
        // Do not execute otherwise (while C# code is being changed).
        _generatedCode ??= ExecuteInternal(context).ToList();

        foreach (var (name, code) in _generatedCode)
        {
            context.AddSource(name, code);
        }
    }

    /// <summary>
    /// Called to perform source generation.
    /// </summary>
    /// <param name="context">The <see cref="GeneratorExecutionContext"/> to add source to.</param>
    /// <returns>Generated code.</returns>
    protected abstract IEnumerable<(string Name, string Code)> ExecuteInternal(GeneratorExecutionContext context);
}
