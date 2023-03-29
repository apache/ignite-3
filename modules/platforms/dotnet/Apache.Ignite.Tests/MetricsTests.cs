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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Internal;
using NUnit.Framework;

/// <summary>
/// Tests <see cref="Metrics"/>.
/// </summary>
public class MetricsTests
{
    [Test]
    public async Task TestConnectionsEstablished()
    {
        using var server = new FakeServer();
        using var listener = new Listener();

        Assert.AreEqual(0, listener.GetMetric("connections-established"));

        (await server.ConnectClientAsync()).Dispose();
        Assert.AreEqual(1, listener.GetMetric("connections-established"));

        (await server.ConnectClientAsync()).Dispose();
        Assert.AreEqual(2, listener.GetMetric("connections-established"));
    }

    private sealed class Listener : IDisposable
    {
        private readonly MeterListener _listener = new();

        private readonly ConcurrentDictionary<string, object> _metrics = new();

        public Listener()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == "Apache.Ignite")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };

            _listener.SetMeasurementEventCallback<int>(HandleEvent);

            _listener.Start();
        }

        public int GetMetric(string name) => _metrics.TryGetValue(name, out var val) ? (int)val : 0;

        public void Dispose()
        {
            _listener.Dispose();
        }

        private void HandleEvent(Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            _metrics.AddOrUpdate(instrument.Name, measurement, (_, val) => (int)val + measurement);
        }
    }
}
