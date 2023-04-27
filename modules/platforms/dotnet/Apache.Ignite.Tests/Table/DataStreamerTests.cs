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

namespace Apache.Ignite.Tests.Table;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Ignite.Table;
using NUnit.Framework;

public class DataStreamerTests : IgniteTestsBase
{
    [Test]
    public async Task DataStreamerExample()
    {
        var table = await Client.Tables.GetTableAsync("foo");

        await table!.RecordBinaryView.StreamDataAsync(ReadHugeFile());
    }

    private static async IAsyncEnumerable<IIgniteTuple> ReadHugeFile()
    {
        await using var file = File.OpenRead("my-file.csv");
        using var reader = new StreamReader(file);

        while (await reader.ReadLineAsync() is { } line)
        {
            // This code block is executed only when "await foreach" iteration is performed in StreamDataAsync,
            // so we don't read more data than we need right now.
            var parts = line.Split(',');

            yield return new IgniteTuple { ["id"] = parts[0], ["name"] = parts[1] };
        }
    }
}
