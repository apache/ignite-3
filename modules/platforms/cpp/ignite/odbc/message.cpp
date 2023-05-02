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

#include "message.h"
#include "utility.h"

#include "streaming/streaming_batch.h"

namespace
{
    using namespace ignite;
    using namespace odbc;

    void ReadAffectedRows(impl::binary::BinaryReaderImpl& reader, const protocol_version& protocolVersion,
        std::vector<int64_t>& affectedRows)
    {
        affectedRows.clear();

        if (protocolVersion < protocol_version::VERSION_2_3_2)
            affectedRows.push_back(reader.ReadInt64());
        else
        {
            int32_t len = reader.ReadInt32();

            affectedRows.reserve(static_cast<size_t>(len));
            for (int32_t i = 0; i < len; ++i)
                affectedRows.push_back(reader.ReadInt64());
        }
    }
}

namespace ignite
{
    namespace odbc
    {
        HandshakeRequest::HandshakeRequest(const config::configuration& config) :
            config(config)
        {
            // No-op.
        }

        HandshakeRequest::~HandshakeRequest()
        {
            // No-op.
        }

        void HandshakeRequest::write(impl::binary::BinaryWriterImpl& writer, const protocol_version&) const
        {
            writer.WriteInt8(RequestType::HANDSHAKE);

            protocol_version version = config.GetProtocolVersion();
            writer.WriteInt16(version.get_major());
            writer.WriteInt16(version.get_minor());
            writer.WriteInt16(version.get_maintenance());

            writer.WriteInt8(ClientType::ODBC);

            writer.WriteBool(config.IsDistributedJoins());
            writer.WriteBool(config.IsEnforceJoinOrder());
            writer.WriteBool(config.IsReplicatedOnly());
            writer.WriteBool(config.IsCollocated());

            if (version >= protocol_version::VERSION_2_1_5)
                writer.WriteBool(config.IsLazy());

            if (version >= protocol_version::VERSION_2_3_0)
                writer.WriteBool(config.IsSkipReducerOnUpdate());

            if (version >= protocol_version::VERSION_2_5_0)
            {
                utility::WriteString(writer, config.GetUser());
                utility::WriteString(writer, config.GetPassword());
            }

            if (version >= protocol_version::VERSION_2_7_0)
                writer.WriteInt8(config.GetNestedTxMode());

            if (version >= protocol_version::VERSION_2_13_0)
            {
                EngineMode::Type mode = config.GetEngineMode();

                if (mode == EngineMode::DEFAULT)
                    writer.WriteString(NULL, 0);

                writer.WriteString(EngineMode::to_string(mode));
            }
        }

        QueryExecuteRequest::QueryExecuteRequest(const std::string& schema, const std::string& sql,
            const parameter_set& params, int32_t timeout, bool autoCommit):
            schema(schema),
            sql(sql),
            params(params),
            timeout(timeout),
            autoCommit(autoCommit)
        {
            // No-op.
        }

        QueryExecuteRequest::~QueryExecuteRequest()
        {
            // No-op.
        }

        void QueryExecuteRequest::write(impl::binary::BinaryWriterImpl& writer, const protocol_version& ver) const
        {
            writer.WriteInt8(RequestType::EXECUTE_SQL_QUERY);

            if (schema.empty())
                writer.WriteNull();
            else
                writer.WriteObject<std::string>(schema);

            writer.WriteObject<std::string>(sql);

            params.write(writer);

            if (ver >= protocol_version::VERSION_2_3_2)
                writer.WriteInt32(timeout);

            if (ver >= protocol_version::VERSION_2_5_0)
                writer.WriteBool(autoCommit);
        }

        QueryExecuteBatchRequest::QueryExecuteBatchRequest(const std::string& schema, const std::string& sql,
            const parameter_set& params, SQLULEN begin, SQLULEN end, bool last, int32_t timeout, bool autoCommit) :
            schema(schema),
            sql(sql),
            params(params),
            begin(begin),
            end(end),
            last(last),
            timeout(timeout),
            autoCommit(autoCommit)
        {
            // No-op.
        }

        QueryExecuteBatchRequest::~QueryExecuteBatchRequest()
        {
            // No-op.
        }

        void QueryExecuteBatchRequest::write(impl::binary::BinaryWriterImpl& writer, const protocol_version& ver) const
        {
            writer.WriteInt8(RequestType::EXECUTE_SQL_QUERY_BATCH);

            if (schema.empty())
                writer.WriteNull();
            else
                writer.WriteObject<std::string>(schema);

            writer.WriteObject<std::string>(sql);

            params.write(writer, begin, end, last);

            if (ver >= protocol_version::VERSION_2_3_2)
                writer.WriteInt32(timeout);

            if (ver >= protocol_version::VERSION_2_5_0)
                writer.WriteBool(autoCommit);
        }

        QueryCloseRequest::QueryCloseRequest(int64_t queryId): queryId(queryId)
        {
            // No-op.
        }

        QueryCloseRequest::~QueryCloseRequest()
        {
            // No-op.
        }

        void QueryCloseRequest::write(impl::binary::BinaryWriterImpl& writer, const protocol_version&) const
        {
            writer.WriteInt8(RequestType::CLOSE_SQL_QUERY);
            writer.WriteInt64(queryId);
        }

        QueryFetchRequest::QueryFetchRequest(int64_t queryId, int32_t page_size):
            queryId(queryId),
            page_size(page_size)
        {
            // No-op.
        }

        QueryFetchRequest::~QueryFetchRequest()
        {
            // No-op.
        }

        void QueryFetchRequest::write(impl::binary::BinaryWriterImpl& writer, const protocol_version&) const
        {
            writer.WriteInt8(RequestType::FETCH_SQL_QUERY);

            writer.WriteInt64(queryId);
            writer.WriteInt32(page_size);
        }

        QueryGetColumnsMetaRequest::QueryGetColumnsMetaRequest(const std::string& schema, const std::string& table,
            const std::string& column):
            schema(schema),
            table(table),
            column(column)
        {
            // No-op.
        }

        QueryGetColumnsMetaRequest::~QueryGetColumnsMetaRequest()
        {
            // No-op.
        }

        void QueryGetColumnsMetaRequest::write(impl::binary::BinaryWriterImpl& writer, const protocol_version&) const
        {
            writer.WriteInt8(RequestType::GET_COLUMNS_METADATA);

            writer.WriteObject<std::string>(schema);
            writer.WriteObject<std::string>(table);
            writer.WriteObject<std::string>(column);
        }

        QueryGetResultsetMetaRequest::QueryGetResultsetMetaRequest(const std::string &schema, const std::string &sqlQuery) :
            schema(schema),
            sqlQuery(sqlQuery)
        {
            // No-op.
        }

        QueryGetResultsetMetaRequest::~QueryGetResultsetMetaRequest()
        {
            // No-op.
        }

        void QueryGetResultsetMetaRequest::write(impl::binary::BinaryWriterImpl &writer, const protocol_version &) const
        {
            writer.WriteInt8(RequestType::META_RESULTSET);

            writer.WriteObject<std::string>(schema);
            writer.WriteObject<std::string>(sqlQuery);
        }

        QueryGetTablesMetaRequest::QueryGetTablesMetaRequest(const std::string& catalog, const std::string& schema,
            const std::string& table, const std::string& tableTypes):
            catalog(catalog),
            schema(schema),
            table(table),
            tableTypes(tableTypes)
        {
            // No-op.
        }

        QueryGetTablesMetaRequest::~QueryGetTablesMetaRequest()
        {
            // No-op.
        }

        void QueryGetTablesMetaRequest::write(impl::binary::BinaryWriterImpl& writer, const protocol_version&) const
        {
            writer.WriteInt8(RequestType::GET_TABLES_METADATA);

            writer.WriteObject<std::string>(catalog);
            writer.WriteObject<std::string>(schema);
            writer.WriteObject<std::string>(table);
            writer.WriteObject<std::string>(tableTypes);
        }

        void QueryGetParamsMetaRequest::write(impl::binary::BinaryWriterImpl& writer, const protocol_version&) const
        {
            writer.WriteInt8(RequestType::GET_PARAMS_METADATA);

            writer.WriteObject<std::string>(schema);
            writer.WriteObject<std::string>(sqlQuery);
        }

        void QueryMoreResultsRequest::write(impl::binary::BinaryWriterImpl& writer, const protocol_version&) const
        {
            writer.WriteInt8(RequestType::QUERY_MORE_RESULTS);

            writer.WriteInt64(queryId);
            writer.WriteInt32(page_size);
        }

        StreamingBatchRequest::StreamingBatchRequest(const std::string& schema,
            const streaming::StreamingBatch& batch, bool last, int64_t order) :
            schema(schema),
            batch(batch),
            last(last),
            order(order)
        {
            // No-op.
        }

        StreamingBatchRequest::~StreamingBatchRequest()
        {
            // No-op.
        }

        void StreamingBatchRequest::write(impl::binary::BinaryWriterImpl& writer, const protocol_version&) const
        {
            writer.WriteInt8(RequestType::STREAMING_BATCH);

            writer.WriteString(schema);

            impl::interop::InteropOutputStream* stream = writer.GetStream();

            writer.WriteInt32(batch.get_size());

            if (batch.get_size() != 0)
                stream->WriteInt8Array(batch.get_data(), batch.GetDataLength());

            writer.WriteBool(last);
            writer.WriteInt64(order);
        }

        Response::Response() :
            status(response_status::UNKNOWN_ERROR),
            error()
        {
            // No-op.
        }

        Response::~Response()
        {
            // No-op.
        }

        void Response::Read(impl::binary::BinaryReaderImpl& reader, const protocol_version& ver)
        {
            if (ver < protocol_version::VERSION_2_1_5)
                status = reader.ReadInt8();
            else
                status = reader.ReadInt32();

            if (status == response_status::SUCCESS)
                ReadOnSuccess(reader, ver);
            else
                utility::ReadString(reader, error);
        }

        void Response::ReadOnSuccess(impl::binary::BinaryReaderImpl&, const protocol_version&)
        {
            // No-op.
        }

        HandshakeResponse::HandshakeResponse():
            accepted(false),
            currentVer(),
            error()
        {
            // No-op.
        }

        HandshakeResponse::~HandshakeResponse()
        {
            // No-op.
        }

        void HandshakeResponse::Read(impl::binary::BinaryReaderImpl& reader, const protocol_version&)
        {
            accepted = reader.ReadBool();

            if (!accepted)
            {
                int16_t major = reader.ReadInt16();
                int16_t minor = reader.ReadInt16();
                int16_t maintenance = reader.ReadInt16();

                currentVer = protocol_version(major, minor, maintenance);

                utility::ReadString(reader, error);
            }
        }

        QueryCloseResponse::QueryCloseResponse(): queryId(0)
        {
            // No-op.
        }

        QueryCloseResponse::~QueryCloseResponse()
        {
            // No-op.
        }

        void QueryCloseResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const protocol_version&)
        {
            queryId = reader.ReadInt64();
        }

        QueryExecuteResponse::QueryExecuteResponse():
            queryId(0),
            meta(),
            affectedRows(0)
        {
            // No-op.
        }

        QueryExecuteResponse::~QueryExecuteResponse()
        {
            // No-op.
        }

        void QueryExecuteResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const protocol_version& ver)
        {
            queryId = reader.ReadInt64();

            meta::read_column_meta_vector(reader, meta, ver);

            ReadAffectedRows(reader, ver, affectedRows);
        }

        QueryExecuteBatchResponse::QueryExecuteBatchResponse() :
            affectedRows(0),
            errorMessage(),
            errorCode(1)
        {
            // No-op.
        }

        QueryExecuteBatchResponse::~QueryExecuteBatchResponse()
        {
            // No-op.
        }

        void QueryExecuteBatchResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const protocol_version& ver)
        {
            bool success = reader.ReadBool();

            ReadAffectedRows(reader, ver, affectedRows);

            if (!success)
            {
                // Ignoring error set idx. To be deleted in next major version.
                reader.ReadInt64();
                errorMessage = reader.ReadObject<std::string>();

                if (ver >= protocol_version::VERSION_2_1_5)
                    errorCode = reader.ReadInt32();
            }
        }

        StreamingBatchResponse::StreamingBatchResponse() :
            errorMessage(),
            errorCode(response_status::SUCCESS),
            order(0)
        {
            // No-op.
        }

        StreamingBatchResponse::~StreamingBatchResponse()
        {
            // No-op.
        }

        void StreamingBatchResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const protocol_version&)
        {
            errorMessage = reader.ReadObject<std::string>();
            errorCode = reader.ReadInt32();
            order = reader.ReadInt64();
        }

        QueryFetchResponse::QueryFetchResponse(ResultPage& resultPage) :
            queryId(0),
            resultPage(resultPage)
        {
            // No-op.
        }

        QueryFetchResponse::~QueryFetchResponse()
        {
            // No-op.
        }

        void QueryFetchResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const protocol_version&)
        {
            queryId = reader.ReadInt64();

            resultPage.Read(reader);
        }

        QueryGetColumnsMetaResponse::QueryGetColumnsMetaResponse()
        {
            // No-op.
        }

        QueryGetColumnsMetaResponse::~QueryGetColumnsMetaResponse()
        {
            // No-op.
        }

        void QueryGetColumnsMetaResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader,
            const protocol_version& ver)
        {
            meta::read_column_meta_vector(reader, meta, ver);
        }

        QueryGetResultsetMetaResponse::QueryGetResultsetMetaResponse()
        {
            // No-op.
        }

        QueryGetResultsetMetaResponse::~QueryGetResultsetMetaResponse()
        {
            // No-op.
        }

        void QueryGetResultsetMetaResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl &reader, const protocol_version& ver)
        {
            meta::read_column_meta_vector(reader, meta, ver);
        }

        QueryGetTablesMetaResponse::QueryGetTablesMetaResponse()
        {
            // No-op.
        }

        QueryGetTablesMetaResponse::~QueryGetTablesMetaResponse()
        {
            // No-op.
        }

        void QueryGetTablesMetaResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const protocol_version&)
        {
            meta::ReadTableMetaVector(reader, meta);
        }

        QueryGetParamsMetaResponse::QueryGetParamsMetaResponse()
        {
            // No-op.
        }

        QueryGetParamsMetaResponse::~QueryGetParamsMetaResponse()
        {
            // No-op.
        }

        void QueryGetParamsMetaResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const protocol_version&)
        {
            utility::ReadByteArray(reader, typeIds);
        }

        QueryMoreResultsResponse::QueryMoreResultsResponse(ResultPage & resultPage) :
            queryId(0),
            resultPage(resultPage)
        {
            // No-op.
        }

        QueryMoreResultsResponse::~QueryMoreResultsResponse()
        {
            // No-op.
        }

        void QueryMoreResultsResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const protocol_version&)
        {
            queryId = reader.ReadInt64();

            resultPage.Read(reader);
        }
    }
}

