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

namespace Apache.Ignite.Internal.Linq.Dml;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using Ignite.Sql;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.ResultOperators;
using Remotion.Linq.Clauses.StreamedData;

/// <summary>
/// Represents an operator for <see cref="IgniteQueryableExtensions.ExecuteDeleteAsync{T}(System.Linq.IQueryable{T})"/>.
/// </summary>
internal sealed class ExecuteDeleteResultOperator : ValueFromSequenceResultOperatorBase
{
    /** <inheritdoc /> */
    public override IStreamedDataInfo GetOutputDataInfo(IStreamedDataInfo inputInfo) =>
        new StreamedScalarValueInfo(typeof(long));

    /** <inheritdoc /> */
    [ExcludeFromCodeCoverage]
    public override ResultOperatorBase Clone(CloneContext cloneContext) =>
        new ExecuteDeleteResultOperator();

    /** <inheritdoc /> */
    [ExcludeFromCodeCoverage]
    public override void TransformExpressions(Func<Expression, Expression> transformation)
    {
        // No-op.
    }

    /** <inheritdoc /> */
    [ExcludeFromCodeCoverage]
    public override StreamedValue ExecuteInMemory<T>(StreamedSequence sequence) =>
        throw new NotSupportedException("ExecuteDelete is not supported for in-memory sequences.");
}
