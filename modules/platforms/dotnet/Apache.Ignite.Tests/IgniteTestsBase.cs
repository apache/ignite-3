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

namespace Apache.Ignite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Common;
    using Common.Table;
    using Ignite.Table;
    using Internal.Proto;
    using Microsoft.Extensions.Logging;
    using NUnit.Framework;
    using static Common.Table.TestTables;

    /// <summary>
    /// Base class for client tests.
    /// <para />
    /// NOTE: Timeout is set for the entire assembly in csproj file.
    /// </summary>
    public class IgniteTestsBase
    {
        protected static readonly TimeSpan ServerIdleTimeout = TimeSpan.FromMilliseconds(3000); // See PlatformTestNodeRunner.

        private static readonly JavaServer ServerNode;

        private readonly List<IDisposable> _disposables = new();

        private TestEventListener _eventListener = null!;

        private ConsoleLogger _logger = null!;

        static IgniteTestsBase()
        {
            JavaServer.SetLogCallback(TestContext.Progress.WriteLine);

            ServerNode = JavaServer.StartAsync().GetAwaiter().GetResult();

            AppDomain.CurrentDomain.ProcessExit += (_, _) => ServerNode.Dispose();
        }

        protected IgniteTestsBase(bool useMapper = false)
        {
            UseMapper = useMapper;
        }

        protected static int ServerPort => ServerNode.Port;

        protected IIgniteClient Client { get; private set; } = null!;

        protected ITable Table { get; private set; } = null!;

        protected IRecordView<IIgniteTuple> TupleView { get; private set; } = null!;

        protected IRecordView<Poco> PocoView { get; private set; } = null!;

        protected IRecordView<PocoAllColumns> PocoAllColumnsView { get; private set; } = null!;

        protected IRecordView<PocoAllColumnsBigDecimal> PocoAllColumnsBigDecimalView { get; private set; } = null!;

        protected IRecordView<PocoAllColumnsNullable> PocoAllColumnsNullableView { get; private set; } = null!;

        protected IRecordView<PocoAllColumnsSql> PocoAllColumnsSqlView { get; private set; } = null!;

        protected IRecordView<PocoAllColumnsSqlNullable> PocoAllColumnsSqlNullableView { get; private set; } = null!;

        protected bool UseMapper { get; }

        protected ConsoleLogger Logger => _logger;

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            _eventListener = new TestEventListener();
            _logger = new ConsoleLogger(LogLevel.Trace);

            Client = await IgniteClient.StartAsync(GetConfig(_logger));

            Table = (await Client.Tables.GetTableAsync(TableName))!;
            TupleView = Table.RecordBinaryView;
            PocoView = UseMapper ? Table.GetRecordView(new PocoMapper()) : Table.GetRecordView<Poco>();

            var tableAllColumns = await Client.Tables.GetTableAsync(TableAllColumnsName);

            PocoAllColumnsNullableView = UseMapper
                ? tableAllColumns!.GetRecordView(new PocoAllColumnsNullableMapper())
                : tableAllColumns!.GetRecordView<PocoAllColumnsNullable>();

            var tableAllColumnsNotNull = await Client.Tables.GetTableAsync(TableAllColumnsNotNullName);

            PocoAllColumnsView = UseMapper
                ? tableAllColumnsNotNull!.GetRecordView(new PocoAllColumnsMapper())
                : tableAllColumnsNotNull!.GetRecordView<PocoAllColumns>();

            PocoAllColumnsBigDecimalView = UseMapper
                ? tableAllColumnsNotNull.GetRecordView(new PocoAllColumnsBigDecimalMapper())
                : tableAllColumnsNotNull.GetRecordView<PocoAllColumnsBigDecimal>();

            var tableAllColumnsSql = await Client.Tables.GetTableAsync(TableAllColumnsSqlName);

            PocoAllColumnsSqlView = UseMapper
                ? tableAllColumnsSql!.GetRecordView(new PocoAllColumnsSqlMapper())
                : tableAllColumnsSql!.GetRecordView<PocoAllColumnsSql>();

            PocoAllColumnsSqlNullableView = UseMapper
                ? tableAllColumnsSql.GetRecordView(new PocoAllColumnsSqlNullableMapper())
                : tableAllColumnsSql.GetRecordView<PocoAllColumnsSqlNullable>();

            _logger.Flush();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            // ReSharper disable once ConstantConditionalAccessQualifier, ConditionalAccessQualifierIsNonNullableAccordingToAPIContract
            Client?.Dispose();

            Assert.Greater(_eventListener.BuffersRented, 0);

            CheckPooledBufferLeak();

            _eventListener.Dispose();
            _logger.Dispose();
        }

        [SetUp]
        public void SetUp()
        {
            _logger.Flush();
            Console.WriteLine("SetUp: " + TestContext.CurrentContext.Test.Name);
            TestUtils.CheckByteArrayPoolLeak();
        }

        [TearDown]
        public void TearDown()
        {
            // Flush here so events from all threads are captured as current test output.
            _logger.Flush();

            Console.WriteLine("TearDown start: " + TestContext.CurrentContext.Test.Name);

            _disposables.ForEach(x => x.Dispose());
            _disposables.Clear();

            CheckPooledBufferLeak();

            _logger.Flush();

            Console.WriteLine("TearDown end: " + TestContext.CurrentContext.Test.Name);
        }

        internal static string GetRequestTargetNodeName(IEnumerable<IgniteProxy> proxies, ClientOp op)
        {
            foreach (var proxy in proxies)
            {
                var ops = proxy.ClientOps;
                proxy.ClearOps();

                if (ops.Contains(op))
                {
                    return proxy.NodeName;
                }
            }

            return string.Empty;
        }

        protected static IIgniteTuple GetTuple(long id) => new IgniteTuple { [KeyCol] = id };

        protected static IIgniteTuple GetTuple(long id, string? val) => new IgniteTuple { [KeyCol] = id, [ValCol] = val };

        protected static IIgniteTuple GetTuple(string? val) => new IgniteTuple { [ValCol] = val };

        protected static Poco GetPoco(long id, string? val = null) => new() { Key = id, Val = val };

        protected static KeyPoco GetKeyPoco(long id) => new() { Key = id };

        protected static ValPoco GetValPoco(string? val) => new() { Val = val };

        protected static IgniteClientConfiguration GetConfig(ILoggerFactory? loggerFactory = null) => new()
        {
            Endpoints =
            {
                "127.0.0.1:" + ServerNode.Port,
                "127.0.0.1:" + (ServerNode.Port + 1)
            },
            LoggerFactory = loggerFactory ?? TestUtils.GetConsoleLoggerFactory(LogLevel.Trace)
        };

        protected static IgniteClientConfiguration GetConfig(IEnumerable<IgniteProxy> proxies, ILoggerFactory loggerFactory) =>
            new(proxies.Select(x => x.Endpoint).ToArray())
            {
                LoggerFactory = loggerFactory
            };

        protected List<IgniteProxy> GetProxies()
        {
            var proxies = Client.GetConnections().Select(c => new IgniteProxy(c.Node.Address, c.Node.Name)).ToList();

            _disposables.AddRange(proxies);

            return proxies;
        }

        private void CheckPooledBufferLeak()
        {
            // Use WaitForCondition to check rented/returned buffers equality:
            // Buffer pools are used by everything, including testing framework, internal .NET needs, etc.
            var listener = _eventListener;
            TestUtils.WaitForCondition(
                condition: () => listener.BuffersReturned == listener.BuffersRented,
                timeoutMs: 1000,
                messageFactory: () => $"rented = {listener.BuffersRented}, returned = {listener.BuffersReturned}");

            TestUtils.CheckByteArrayPoolLeak();
        }
    }
}
