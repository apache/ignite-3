﻿/*
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
    using System.Runtime.Serialization;

    /// <summary>
    /// Ignite thin client exception.
    /// </summary>
    [Serializable]
    public class IgniteClientException : Exception
    {
        /** Error code field. */
        private const string ErrorCodeField = "StatusCode";

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientException"/> class.
        /// </summary>
        public IgniteClientException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientException" /> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public IgniteClientException(string message)
            : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientException" /> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public IgniteClientException(string message, Exception? cause)
            : base(message, cause)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientException" /> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        /// <param name="statusCode">The error code.</param>
        public IgniteClientException(string message, Exception? cause, int statusCode)
            : base(message, cause)
        {
            ErrorCode = statusCode;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected IgniteClientException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            ErrorCode = info.GetInt32(ErrorCodeField);
        }

        /// <summary>
        /// Gets the error code.
        /// </summary>
        public int ErrorCode { get; }

        /// <inheritdoc />
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            info.AddValue(ErrorCodeField, ErrorCode);
        }
    }
}
