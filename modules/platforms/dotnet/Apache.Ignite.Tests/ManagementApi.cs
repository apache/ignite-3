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

namespace Apache.Ignite.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

/// <summary>
/// Ignite management REST API wrapper.
/// </summary>
public class ManagementApi(Uri baseUrl)
{
    public async Task<string> UnitDeploy(string unitId, string unitVersion, List<string> unitContent)
    {
        // See DeployUnitClient.java
        var url = new UriBuilder(baseUrl)
        {
            Path = $"/management/v1/deployment/units/{Uri.EscapeDataString(unitId)}/{Uri.EscapeDataString(unitVersion)}"
        };

        var content = new MultipartFormDataContent();
        foreach (var file in unitContent)
        {
            // HttpClient will close the file.
            var fileContent = new StreamContent(File.OpenRead(file));
            fileContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            content.Add(fileContent, "unitContent", file);
        }

        var request = new HttpRequestMessage(HttpMethod.Post, url.ToString())
        {
            Content = content
        };

        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        using var client = new HttpClient();
        var response = await client.SendAsync(request);

        return await response.Content.ReadAsStringAsync();
    }
}
