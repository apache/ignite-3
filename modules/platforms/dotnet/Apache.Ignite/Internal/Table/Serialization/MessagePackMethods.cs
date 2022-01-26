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

namespace Apache.Ignite.Internal.Table.Serialization
{
    using System;
    using System.Reflection;
    using MessagePack;
    using Proto;

    /// <summary>
    /// MethodInfos for <see cref="MessagePackWriter"/> and <see cref="MessagePackReader"/>.
    /// </summary>
    internal static class MessagePackMethods
    {
        /// <summary>
        /// String writer.
        /// </summary>
        public static readonly MethodInfo WriteString = GetWriteMethod<string>();

        /// <summary>
        /// Int writer.
        /// </summary>
        public static readonly MethodInfo WriteInt = GetWriteMethod<int>();

        /// <summary>
        /// No-value writer.
        /// </summary>
        public static readonly MethodInfo WriteNoValue =
            typeof(MessagePackWriterExtensions).GetMethod(nameof(MessagePackWriterExtensions.WriteNoValue))!;

        /// <summary>
        /// Object (catch all) writer.
        /// </summary>
        public static readonly MethodInfo WriteObject =
            typeof(MessagePackWriterExtensions).GetMethod(nameof(MessagePackWriterExtensions.WriteObject))!;

        private static MethodInfo GetWriteMethod<TArg>()
        {
            var methodInfo = typeof(MessagePackWriter).GetMethod(
                nameof(MessagePackWriter.Write), new[] { typeof(TArg) });

            if (methodInfo == null)
            {
                throw new InvalidOperationException($"Method not found: Write({typeof(TArg).Name})");
            }

            return methodInfo;
        }
    }
}
