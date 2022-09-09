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

namespace Apache.Ignite.Tests
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization.Formatters.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests Ignite exceptions.
    /// </summary>
    public class ExceptionsTests
    {
        /// <summary>
        /// Tests that all exceptions have mandatory constructors and are serializable.
        /// </summary>
        [Test]
        public void TestExceptionsAreSerializableAndHaveRequiredConstructors()
        {
            var types = typeof(IIgnite).Assembly.GetTypes().Where(x => x.IsSubclassOf(typeof(Exception)));

            foreach (var type in types)
            {
                Assert.IsTrue(type.IsSerializable, "Exception is not serializable: " + type);

                // Check required IgniteException constructor.
                var defCtor = type.GetConstructor(new[] { typeof(Guid), typeof(int), typeof(string), typeof(Exception) });
                Assert.IsNotNull(defCtor, "Required constructor is missing: " + type);

                var ex = (Exception)defCtor!.Invoke(new object[] { Guid.NewGuid(), 1, "msg", new Exception() });
                Assert.AreEqual("msg", ex.Message);

                // Serialization.
                var stream = new MemoryStream();
                var formatter = new BinaryFormatter();

                formatter.Serialize(stream, ex);
                stream.Seek(0, SeekOrigin.Begin);

                ex = (Exception) formatter.Deserialize(stream);
                Assert.AreEqual("myMessage", ex.Message);

                // Message+cause ctor.
                var msgCauseCtor = type.GetConstructor(new[] { typeof(string), typeof(Exception) })!;
                Assert.IsNotNull(msgCauseCtor);

                ex = (Exception) msgCauseCtor.Invoke(new object[] {"myMessage", new Exception("innerEx")});
                Assert.AreEqual("myMessage", ex.Message);
                Assert.IsNotNull(ex.InnerException);
                Assert.AreEqual("innerEx", ex.InnerException!.Message);
            }
        }
    }
}
