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

public static class Assert
{
    public static void AreEqual(object? expected, object? actual)
    {
        if (expected is byte[] expectedBytes && actual is byte[] actualBytes)
        {
            if (expectedBytes.Length != actualBytes.Length)
            {
                throw new Exception($"Assert.AreEqual failed. Expected byte array length: {expectedBytes.Length}, Actual byte array length: {actualBytes.Length}");
            }

            for (int i = 0; i < expectedBytes.Length; i++)
            {
                if (expectedBytes[i] != actualBytes[i])
                {
                    throw new Exception($"Assert.AreEqual failed. Byte arrays differ at index {i}. Expected: {expectedBytes[i]}, Actual: {actualBytes[i]}");
                }
            }

            return;
        }

        if (!Equals(expected, actual))
        {
            throw new Exception($"Assert.AreEqual failed. Expected: {expected}, Actual: {actual}");
        }
    }
}
