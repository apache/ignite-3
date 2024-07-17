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
    using Internal.Common;

    /// <summary>
    /// Ignite exception.
    /// </summary>
    [Serializable]
    [SuppressMessage(
        "Microsoft.Design",
        "CA1032:ImplementStandardExceptionConstructors",
        Justification="Ignite exceptions use a special constructor.")]
    public class IgniteException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteException"/> class.
        /// </summary>
        /// <param name="traceId">Trace id.</param>
        /// <param name="code">Code.</param>
        /// <param name="message">Message.</param>
        /// <param name="innerException">Inner exception.</param>
        public IgniteException(Guid traceId, int code, string? message, Exception? innerException = null)
            : base(message, innerException)
        {
            TraceId = traceId;
            Code = code;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="IgniteException"/> class.
        /// </summary>
        /// <param name="serializationInfo">Serialization information.</param>
        /// <param name="streamingContext">Streaming context.</param>
        protected IgniteException(SerializationInfo serializationInfo, StreamingContext streamingContext)
            : base(serializationInfo, streamingContext)
        {
            IgniteArgumentCheck.NotNull(serializationInfo);

            TraceId = (Guid)serializationInfo.GetValue(nameof(TraceId), typeof(Guid))!;
            Code = serializationInfo.GetInt32(nameof(Code));
        }

        /// <summary>
        /// Gets the group name.
        /// </summary>
        public string GroupName => ErrorGroups.GetGroupName(ErrorGroups.GetGroupCode(Code));

        /// <summary>
        /// Gets the full exception code.
        /// </summary>
        public int Code { get; }

        /// <summary>
        /// Gets the trace id (correlation id).
        /// </summary>
        public Guid TraceId { get; }

        /// <summary>
        /// Gets the error code.
        /// </summary>
        public short ErrorCode => ErrorGroups.GetErrorCode(Code);

        /// <summary>
        /// Gets the code as string.
        /// </summary>
        public string CodeAsString => ErrorGroups.ErrPrefix + GroupName + '-' + ErrorCode;

        /// <inheritdoc />
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            info.AddValue(nameof(Code), Code);
            info.AddValue(nameof(TraceId), TraceId);
        }
    }
}
