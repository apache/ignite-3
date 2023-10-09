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
    using Internal.Common;

    /// <summary>
    /// Retry policy that returns <c>true</c> when <see cref="IRetryPolicyContext.Iteration"/> is less than
    /// the specified <see cref="RetryLimit"/>, or the limit is zero or less.
    /// </summary>
    public class RetryLimitPolicy : IRetryPolicy
    {
        /// <summary>
        /// Default retry limit.
        /// </summary>
        public const int DefaultRetryLimit = 16;

        /// <summary>
        /// Gets or sets the retry limit. 0 or less for no limit. Default is <see cref="DefaultRetryLimit"/>.
        /// </summary>
        public int RetryLimit { get; set; } = DefaultRetryLimit;

        /// <inheritdoc />
        public virtual bool ShouldRetry(IRetryPolicyContext context)
        {
            IgniteArgumentCheck.NotNull(context);

            if (RetryLimit <= 0)
            {
                return true;
            }

            return context.Iteration < RetryLimit;
        }

        /// <inheritdoc />
        public override string ToString() =>
            new IgniteToStringBuilder(GetType())
                .Append(RetryLimit)
                .Build();
    }
}
