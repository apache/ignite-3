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
    using Ignite.Table;
    using Internal.Proto;
    using Microsoft.Extensions.Logging;
    using NUnit.Framework;
    using Table;

    /// <summary>
    /// Base class for client tests.
    /// <para />
    /// NOTE: Timeout is set for the entire assembly in csproj file.
    /// </summary>
    public class IgniteTestsBase
    {
        protected const string TableName = "TBL1";
        protected const int TablePartitionCount = 10;

        protected const string TableAllColumnsName = "TBL_ALL_COLUMNS";
        protected const string TableAllColumnsNotNullName = "TBL_ALL_COLUMNS_NOT_NULL";
        protected const string TableAllColumnsSqlName = "TBL_ALL_COLUMNS_SQL";

        protected const string TableInt8Name = "TBL_INT8";
        protected const string TableBoolName = "TBL_BOOLEAN";
        protected const string TableInt16Name = "TBL_INT16";
        protected const string TableInt32Name = "TBL_INT32";
        protected const string TableInt64Name = "TBL_INT64";
        protected const string TableFloatName = "TBL_FLOAT";
        protected const string TableDoubleName = "TBL_DOUBLE";
        protected const string TableDecimalName = "TBL_DECIMAL";
        protected const string TableStringName = "TBL_STRING";
        protected const string TableDateName = "TBL_DATE";
        protected const string TableDateTimeName = "TBL_DATETIME";
        protected const string TableTimeName = "TBL_TIME";
        protected const string TableTimestampName = "TBL_TIMESTAMP";
        protected const string TableNumberName = "TBL_NUMBER";
        protected const string TableBytesName = "TBL_BYTE_ARRAY";

        protected const string KeyCol = "key";

        protected const string ValCol = "val";

        protected static readonly TimeSpan ServerIdleTimeout = TimeSpan.FromMilliseconds(3000); // See PlatformTestNodeRunner.

        private static readonly JavaServer ServerNode;

        private readonly List<IDisposable> _disposables = new();

        private TestEventListener _eventListener = null!;

        private ConsoleLogger _logger = null!;

        static IgniteTestsBase()
        {
            ServerNode = JavaServer.StartAsync().GetAwaiter().GetResult();

            AppDomain.CurrentDomain.ProcessExit += (_, _) => ServerNode.Dispose();
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

        [OneTimeSetUp]
        public async Task OneTimeSetUp()
        {
            _eventListener = new TestEventListener();
            _logger = new ConsoleLogger(LogLevel.Trace);

            Client = await IgniteClient.StartAsync(GetConfig(_logger));

            Table = (await Client.Tables.GetTableAsync(TableName))!;
            TupleView = Table.RecordBinaryView;
            PocoView = Table.GetRecordView<Poco>();

            var tableAllColumns = await Client.Tables.GetTableAsync(TableAllColumnsName);
            PocoAllColumnsNullableView = tableAllColumns!.GetRecordView<PocoAllColumnsNullable>();

            var tableAllColumnsNotNull = await Client.Tables.GetTableAsync(TableAllColumnsNotNullName);
            PocoAllColumnsView = tableAllColumnsNotNull!.GetRecordView<PocoAllColumns>();
            PocoAllColumnsBigDecimalView = tableAllColumnsNotNull.GetRecordView<PocoAllColumnsBigDecimal>();

            var tableAllColumnsSql = await Client.Tables.GetTableAsync(TableAllColumnsSqlName);
            PocoAllColumnsSqlView = tableAllColumnsSql!.GetRecordView<PocoAllColumnsSql>();
            PocoAllColumnsSqlNullableView = tableAllColumnsSql.GetRecordView<PocoAllColumnsSqlNullable>();

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
