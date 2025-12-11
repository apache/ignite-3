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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Tasks;
using Apache.Ignite.Compute;
using Common.Compute;
using Compute;
using Internal.Common;
using NUnit.Framework;

/// <summary>
/// Ignite management REST API wrapper.
/// </summary>
public static class ManagementApi
{
    private const string BaseUri = "http://localhost:10300";

    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public static async Task<DeploymentUnit> UnitDeploy(string unitId, string unitVersion, IList<string> unitContent)
    {
        // See DeployUnitClient.java
        var url = GetUnitUrl(unitId, unitVersion);

        var content = new MultipartFormDataContent();
        foreach (var file in unitContent)
        {
            // HttpClient will close the file.
            var fileContent = new StreamContent(File.OpenRead(file));
            fileContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            content.Add(fileContent, "unitContent", fileName: Path.GetFileName(file));
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

        await TestUtils.WaitForConditionAsync(
            async () =>
            {
                var statuses = await GetUnitStatus(unitId);

                return statuses != null &&
                       statuses.Any(status => status.VersionToStatus.Any(v => v.Version == unitVersion && v.Status == "DEPLOYED"));
            },
            timeoutMs: 5000,
            () => $"Failed to deploy unit {unitId} version {unitVersion}: {GetUnitStatusString()}");

        return new DeploymentUnit(unitId, unitVersion);

        string? GetUnitStatusString() =>
            GetUnitStatus(unitId).GetAwaiter().GetResult()?
                .SelectMany(x => x.VersionToStatus)
                .StringJoin();
    }

    public static async Task UnitUndeploy(DeploymentUnit? unit)
    {
        if (unit == null)
        {
            return;
        }

        using var client = new HttpClient();
        await client.DeleteAsync(GetUnitUrl(unit.Name, unit.Version).Uri);
    }

    public static async Task<DeploymentUnit> DeployTestsAssembly(string? unitId = null, string? unitVersion = null)
    {
        using var tempDir = new TempDir();
        var testsDll = typeof(ManagementApi).Assembly.Location;
        var newerDotNetDll = await DotNetJobs.WriteNewerDotnetJobsAssembly(tempDir.Path, "NewerDotnetJobs");

        var unitId0 = unitId ?? TestContext.CurrentContext.Test.FullName;
        var unitVersion0 = unitVersion ?? GetRandomUnitVersion();

        return await UnitDeploy(
            unitId: unitId0,
            unitVersion: unitVersion0,
            unitContent: [testsDll, newerDotNetDll]);
    }

    public static string GetRandomUnitVersion() => DateTime.Now.TimeOfDay.ToString(@"m\.s\.f");

    private static async Task<DeploymentUnitStatus[]?> GetUnitStatus(string unitId)
    {
        using var client = new HttpClient();
        using var response = await client.GetAsync(GetUnitClusterUrl(unitId).Uri);

        await using var responseStream = await response.Content.ReadAsStreamAsync();
        return await JsonSerializer.DeserializeAsync<DeploymentUnitStatus[]>(responseStream, JsonSerializerOptions);
    }

    private static UriBuilder GetUnitUrl(string unitId, string unitVersion) =>
        new(BaseUri) { Path = $"/management/v1/deployment/units/{Uri.EscapeDataString(unitId)}/{Uri.EscapeDataString(unitVersion)}" };

    private static UriBuilder GetUnitClusterUrl(string unitId) =>
        new(BaseUri) { Path = $"/management/v1/deployment/cluster/units/{Uri.EscapeDataString(unitId)}" };

    private record DeploymentUnitStatus(string Id, DeploymentUnitVersionStatus[] VersionToStatus);

    private record DeploymentUnitVersionStatus(string Version, string Status);
}
