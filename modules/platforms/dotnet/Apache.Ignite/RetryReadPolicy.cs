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

namespace Apache.Ignite
{
    using System;
    using Internal.Common;

    /// <summary>
    /// Retry policy that returns true for all read-only operations that do not modify data.
    /// </summary>
    public sealed class RetryReadPolicy : RetryLimitPolicy
    {
        /// <inheritdoc />
        public override bool ShouldRetry(IRetryPolicyContext context)
        {
            IgniteArgumentCheck.NotNull(context);

            if (!base.ShouldRetry(context))
            {
                return false;
            }

            return context.Operation switch
            {
                ClientOperationType.TablesGet => true,
                ClientOperationType.TableGet => true,
                ClientOperationType.TupleUpsert => false,
                ClientOperationType.TupleGet => true,
                ClientOperationType.TupleUpsertAll => false,
                ClientOperationType.TupleGetAll => true,
                ClientOperationType.TupleGetAndUpsert => false,
                ClientOperationType.TupleInsert => false,
                ClientOperationType.TupleInsertAll => false,
                ClientOperationType.TupleReplace => false,
                ClientOperationType.TupleReplaceExact => false,
                ClientOperationType.TupleGetAndReplace => false,
                ClientOperationType.TupleDelete => false,
                ClientOperationType.TupleDeleteAll => false,
                ClientOperationType.TupleDeleteExact => false,
                ClientOperationType.TupleDeleteAllExact => false,
                ClientOperationType.TupleGetAndDelete => false,
                ClientOperationType.TupleContainsKey => false,
                ClientOperationType.ComputeExecute => false,
                ClientOperationType.ComputeExecuteMapReduce => false,
                ClientOperationType.SqlExecute => false,
                ClientOperationType.SqlExecuteScript => false,
                ClientOperationType.ComputeCancel => false,
                ClientOperationType.ComputeChangePriority => false,
                ClientOperationType.ComputeGetStatus => true,
                ClientOperationType.StreamerBatchSend => false,
                ClientOperationType.StreamerWithReceiverBatchSend => false,
                ClientOperationType.PrimaryReplicasGet => true,
                var unsupported => throw new NotSupportedException("Unsupported operation type: " + unsupported)
            };
        }
    }
}
