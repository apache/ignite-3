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

namespace Apache.Ignite.Tests.Aot;

using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Common;

public class TestRunner(ConsoleLogger logger)
{
    public int TestCount { get; private set; }

    public async Task Run<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods)] T>(T testClass)
    {
        var methods = typeof(T).GetMethods(BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);

        foreach (var method in methods)
        {
            Console.WriteLine($">>> Starting {method.Name}...");
            TestCount++;

            try
            {
                var result = method.Invoke(testClass, []);
                if (result is Task task)
                {
                    await task;
                }

                logger.Flush();
                Console.WriteLine($">>> {method.Name} passed.");
            }
            catch (Exception)
            {
                logger.Flush();
                Console.WriteLine($">>> {method.Name} failed.");
                throw;
            }
        }
    }
}
