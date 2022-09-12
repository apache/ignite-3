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
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.Serialization;

    /// <summary>
    /// Ignite thin client exception.
    /// </summary>
    [Serializable]
    [SuppressMessage(
        "Microsoft.Design",
        "CA1032:ImplementStandardExceptionConstructors",
        Justification="Ignite exceptions use a special constructor.")]
    public class IgniteClientConnectionException : IgniteClientException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConnectionException"/> class.
        /// </summary>
        /// <param name="code">Code.</param>
        /// <param name="message">Message.</param>
        /// <param name="innerException">Inner exception.</param>
        public IgniteClientConnectionException(int code, string message, Exception? innerException = null)
            : base(Guid.NewGuid(), code, message, innerException)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConnectionException"/> class.
        /// </summary>
        /// <param name="traceId">Trace id.</param>
        /// <param name="code">Code.</param>
        /// <param name="message">Message.</param>
        /// <param name="innerException">Inner exception.</param>
        public IgniteClientConnectionException(Guid traceId, int code, string message, Exception? innerException = null)
            : base(traceId, code, message, innerException)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteClientConnectionException"/> class.
        /// </summary>
        /// <param name="serializationInfo">Serialization information.</param>
        /// <param name="streamingContext">Streaming context.</param>
        protected IgniteClientConnectionException(SerializationInfo serializationInfo, StreamingContext streamingContext)
            : base(serializationInfo, streamingContext)
        {
            // No-op.
        }
    }
}
