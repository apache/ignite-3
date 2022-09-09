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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Text.RegularExpressions;
    using NUnit.Framework;

    /// <summary>
    /// Tests Ignite exceptions.
    /// </summary>
    public class ExceptionsTests
    {
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

        [Test]
        public void TestAllJavaIgniteExceptionsHaveDotNetCounterparts()
        {
            var modulesDir = Path.GetFullPath(Path.Combine(TestUtils.RepoRootDir, "modules"));

            var javaExceptionsWithParents = Directory.EnumerateFiles(modulesDir, "*Exception.java", SearchOption.AllDirectories)
                .Where(x => !x.Contains("internal"))
                .Select(File.ReadAllText)
                .Select(x => Regex.Match(x, @"public class (\w+) extends (\w+)"))
                .Where(x => x.Success && !x.Value.Contains("RaftException")) // Ignore duplicate RaftException.
                .ToDictionary(x => x.Groups[1].Value, x => x.Groups[2].Value);

            Assert.IsNotEmpty(javaExceptionsWithParents);

            var javaExceptions = javaExceptionsWithParents.Select(x => x.Key).Where(IsIgniteException).ToList();

            Assert.IsNotEmpty(javaExceptions);

            Assert.Multiple(() =>
            {
                foreach (var exception in javaExceptions)
                {
                    var dotNetException = typeof(IIgnite).Assembly.GetType(exception, throwOnError: false);

                    Assert.IsNotNull(dotNetException, "No .NET equivalent for Java exception: " + exception);
                }
            });

            bool IsIgniteException(string? ex) =>
                ex != null && (ex == "IgniteException" || IsIgniteException(javaExceptionsWithParents.GetValueOrDefault(ex)));
        }
    }
}
