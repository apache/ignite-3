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

namespace Apache.Ignite.Internal.Proto
{
    using System.Text;
    using MessagePack;

    /// <summary>
    /// Extension methods for <see cref="MessagePackWriter"/>.
    /// </summary>
    internal static class MessagePackWriterExtensions
    {
        /// <summary>
        /// Writes a string.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="str">String.</param>
        public static void WriteString(this ref MessagePackWriter writer, string str)
        {
            // TODO: Use array pool or stackalloc for small strings.
            writer.WriteString(Encoding.UTF8.GetBytes(str));
        }
    }
}
