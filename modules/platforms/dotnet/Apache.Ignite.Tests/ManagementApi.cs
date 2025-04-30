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
public static class ManagementApi
{
    public static async Task UnitDeploy(string unitId, string unitVersion, IList<string> unitContent)
    {
        // See DeployUnitClient.java
        var url = new UriBuilder("localhost:10300")
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
        HttpResponseMessage response = await client.SendAsync(request);
        string resContent = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            throw new Exception($"Failed to deploy unit. Status code: {response.StatusCode}, Content: {resContent}");
        }
    }
}
