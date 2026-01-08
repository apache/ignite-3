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

namespace Apache.Ignite.Tests.Sql;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Ignite.Sql;
using NUnit.Framework;

public class IgniteDbConnectionStringBuilderTests
{
    [Test]
    public void TestParseFullConnectionString()
    {
        var connStr =
            "Endpoints=localhost:10800,localhost:10801;SocketTimeout=00:00:02.5000000;OperationTimeout=00:01:14.0700000;" +
            "HeartbeatInterval=00:00:01.3640000;ReconnectInterval=00:00:00.5432100;SslEnabled=True;Username=user1;Password=hunter2;" +
            "ReResolveAddressesInterval=11:22:33";

        var builder = new IgniteDbConnectionStringBuilder(connStr);

        CollectionAssert.AreEquivalent(new[] {"localhost:10800", "localhost:10801"}, builder.Endpoints);
        Assert.AreEqual(TimeSpan.FromSeconds(2.5), builder.SocketTimeout);
        Assert.AreEqual(TimeSpan.FromMinutes(1.2345), builder.OperationTimeout);
        Assert.AreEqual(TimeSpan.FromSeconds(1.364), builder.HeartbeatInterval);
        Assert.AreEqual(TimeSpan.FromSeconds(0.54321), builder.ReconnectInterval);
        Assert.AreEqual(new TimeSpan(0, 11, 22, 33), builder.ReResolveAddressesInterval);
        Assert.IsTrue(builder.SslEnabled);
        Assert.AreEqual("user1", builder.Username);
        Assert.AreEqual("hunter2", builder.Password);

        Assert.AreEqual(connStr.ToLowerInvariant(), builder.ToString().ToLowerInvariant());

        IgniteClientConfiguration clientConfig = builder.ToIgniteClientConfiguration();

        CollectionAssert.AreEquivalent(new[] {"localhost:10800", "localhost:10801"}, clientConfig.Endpoints);
        Assert.AreEqual(TimeSpan.FromSeconds(2.5), clientConfig.SocketTimeout);
        Assert.AreEqual(TimeSpan.FromMinutes(1.2345), clientConfig.OperationTimeout);
        Assert.AreEqual(TimeSpan.FromSeconds(1.364), clientConfig.HeartbeatInterval);
        Assert.AreEqual(TimeSpan.FromSeconds(0.54321), clientConfig.ReconnectInterval);
        Assert.AreEqual(new TimeSpan(0, 11, 22, 33), clientConfig.ReResolveAddressesInterval);
        Assert.IsNotNull(clientConfig.SslStreamFactory);
        Assert.AreEqual("user1", ((BasicAuthenticator)clientConfig.Authenticator!).Username);
        Assert.AreEqual("hunter2", ((BasicAuthenticator)clientConfig.Authenticator!).Password);
    }

    [Test]
    public void TestParseMinimalConnectionString()
    {
        var connStr = "endpoints=foobar:1234";

        var builder = new IgniteDbConnectionStringBuilder(connStr);

        CollectionAssert.AreEquivalent(new[] {"foobar:1234"}, builder.Endpoints);
        Assert.AreEqual(IgniteClientConfiguration.DefaultSocketTimeout, builder.SocketTimeout);
        Assert.AreEqual(IgniteClientConfiguration.DefaultOperationTimeout, builder.OperationTimeout);
        Assert.AreEqual(IgniteClientConfiguration.DefaultHeartbeatInterval, builder.HeartbeatInterval);
        Assert.AreEqual(IgniteClientConfiguration.DefaultReconnectInterval, builder.ReconnectInterval);
        Assert.AreEqual(IgniteClientConfiguration.DefaultReResolveAddressesInterval, builder.ReResolveAddressesInterval);
        Assert.IsFalse(builder.SslEnabled);
        Assert.IsNull(builder.Username);
        Assert.IsNull(builder.Password);

        Assert.AreEqual(connStr, builder.ToString());

        IgniteClientConfiguration clientConfig = builder.ToIgniteClientConfiguration();

        CollectionAssert.AreEquivalent(new[] {"foobar:1234"}, clientConfig.Endpoints);
        Assert.AreEqual(IgniteClientConfiguration.DefaultSocketTimeout, clientConfig.SocketTimeout);
        Assert.AreEqual(IgniteClientConfiguration.DefaultOperationTimeout, clientConfig.OperationTimeout);
        Assert.AreEqual(IgniteClientConfiguration.DefaultHeartbeatInterval, clientConfig.HeartbeatInterval);
        Assert.AreEqual(IgniteClientConfiguration.DefaultReconnectInterval, clientConfig.ReconnectInterval);
        Assert.AreEqual(IgniteClientConfiguration.DefaultReResolveAddressesInterval, clientConfig.ReResolveAddressesInterval);
        Assert.IsNull(clientConfig.SslStreamFactory);
        Assert.IsNull(clientConfig.Authenticator);
    }

    [Test]
    public void TestToStringBuildsFullConnectionString()
    {
        var builder = new IgniteDbConnectionStringBuilder
        {
            Endpoints = ["localhost:10800", "localhost:10801"],
            SocketTimeout = TimeSpan.FromSeconds(2.5),
            OperationTimeout = TimeSpan.FromMinutes(1.2345),
            HeartbeatInterval = TimeSpan.FromSeconds(1.364),
            ReconnectInterval = TimeSpan.FromSeconds(0.54321),
            SslEnabled = true,
            Username = "user1",
            Password = "hunter2"
        };

        Assert.AreEqual(
            "Endpoints=localhost:10800,localhost:10801;SocketTimeout=00:00:02.5000000;OperationTimeout=00:01:14.0700000;" +
            "HeartbeatInterval=00:00:01.3640000;ReconnectInterval=00:00:00.5432100;SslEnabled=True;Username=user1;Password=hunter2",
            builder.ToString());
    }

    [Test]
    public void TestToStringBuildsMinimalConnectionString()
    {
        var builder = new IgniteDbConnectionStringBuilder
        {
            Endpoints = ["foo:123"]
        };

        Assert.AreEqual("Endpoints=foo:123", builder.ToString());
    }

    [Test]
    public void TestBuilderHasSamePropertiesAsClientConfig()
    {
        var builderProps = typeof(IgniteDbConnectionStringBuilder).GetProperties();
        var configProps = typeof(IgniteClientConfiguration).GetProperties();

        foreach (var configProp in configProps)
        {
            if (configProp.Name is nameof(IgniteClientConfiguration.LoggerFactory)
                or nameof(IgniteClientConfiguration.RetryPolicy)
                or nameof(IgniteClientConfiguration.Authenticator)
                or nameof(IgniteClientConfiguration.SslStreamFactory))
            {
                // Not supported yet.
                continue;
            }

            var builderProp = builderProps.SingleOrDefault(x => x.Name == configProp.Name);
            Assert.NotNull(builderProp, $"Property '{configProp.Name}' not found in IgniteDbConnectionStringBuilder");
            Assert.AreEqual(configProp.PropertyType, builderProp!.PropertyType, $"Property '{configProp.Name}' type mismatch");
        }
    }

    [Test]
    [SuppressMessage("ReSharper", "ObjectCreationAsStatement", Justification = "Tests")]
    public void TestUnknownConnectionStringPropertyThrows()
    {
        var connStr = "foo=bar;Endpoints=localhost:10800";

        var ex = Assert.Throws<ArgumentException>(() => new IgniteDbConnectionStringBuilder(connStr));
        Assert.AreEqual("Unknown connection string key: 'foo'. (Parameter 'keyword')", ex.Message);
    }

    [Test]
    [SuppressMessage("ReSharper", "CollectionNeverQueried.Local", Justification = "Tests")]
    public void TestUnknownSetterPropertyThrows()
    {
        var builder = new IgniteDbConnectionStringBuilder();

        var ex = Assert.Throws<ArgumentException>(() => builder["baz"] = "bar");
        Assert.AreEqual("Unknown connection string key: 'baz'. (Parameter 'keyword')", ex.Message);
    }
}
