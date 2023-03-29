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
using System.Diagnostics.CodeAnalysis;
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

        (await server.ConnectClientAsync()).Dispose();
        (await server.ConnectClientAsync()).Dispose();

        var events = listener.Events.ToArray();
        Assert.AreEqual(2, events.Length);
        Assert.AreEqual(new MetricsEvent("connections-established", 1), events[0]);
        Assert.AreEqual(new MetricsEvent("connections-established", 1), events[1]);
    }

    private sealed class Listener : IDisposable
    {
        private readonly MeterListener _listener = new();

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

        public ConcurrentQueue<MetricsEvent> Events { get; } = new();

        public void Dispose()
        {
            _listener.Dispose();
        }

        private void HandleEvent(Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) =>
            Events.Enqueue(new MetricsEvent(instrument.Name, measurement));
    }

    [SuppressMessage("ReSharper", "NotAccessedPositionalProperty.Local", Justification = "Record.")]
    private record MetricsEvent(string InstrumentName, int Measurement);
}
