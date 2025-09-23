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

namespace Apache.Ignite.Tests.TestHelpers;

using System;
using System.IO;
using System.Threading.Tasks;

/// <summary>
/// Temporary directory holder for tests.
/// </summary>
public sealed record TempDir : IDisposable
{
    public TempDir()
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), Guid.NewGuid().ToString());

        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public void Dispose() => _ = Task.Run(() =>
    {
        // Delete test temp dir in background, ignore errors.
        Directory.Delete(Path, true);
    });
}
