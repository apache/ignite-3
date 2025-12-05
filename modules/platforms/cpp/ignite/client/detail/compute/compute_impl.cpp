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

#include "ignite/client/detail/compute/compute_impl.h"
#include "ignite/client/detail/table/table_impl.h"

#include "colocated_job_target.h"
#include "ignite/client/detail/argument_check_utils.h"
#include "ignite/client/detail/compute/job_execution_impl.h"
#include "ignite/client/detail/utils.h"
#include "ignite/protocol/utils.h"
#include "ignite/tuple/binary_tuple_builder.h"

namespace ignite::detail {

/**
 * Compute Job Type.
 */
enum class compute_job_type {
    /** Native. */
    NATIVE = 0,

    /** Marshalled Tuple. */
    MARSHALLED_TUPLE = 1,

    /** Marshalled Object. */
    MARSHALLED_OBJECT = 2,
};


/**
 * Get Compute Job Type from Type ID.
 * @param type_id Type ID.
 * @return Compute Job Type.
 */
compute_job_type from_type_id(ignite_type type_id) {
    auto type_conv = compute_job_type(type_id);

    if (type_conv == compute_job_type::MARSHALLED_TUPLE || type_conv == compute_job_type::MARSHALLED_OBJECT)
        return type_conv;

    return compute_job_type::NATIVE;
}


/**
 * Write a collection of primitives as a binary tuple.
 *
 * @param writer Writer to use.
 * @param arg Argument.
 */
void write_object_as_binary_tuple(protocol::writer &writer, const primitive &arg) {
    binary_tuple_builder args_builder{3};

    args_builder.start();
    protocol::claim_primitive_with_type(args_builder, arg);
    args_builder.layout();
    protocol::append_primitive_with_type(args_builder, arg);

    auto args_data = args_builder.build();
    writer.write_binary(args_data);
}

/**
 * Pack compute argument.
 *
 * @param writer Writer.
 * @param arg Argument.
 */
void pack_compute_argument(protocol::writer &writer, const binary_object &arg) {
    auto prim = arg.get_primitive();
    if (prim.is_null()) {
        writer.write_nil();
        return;
    }

    writer.write(std::int32_t(compute_job_type::NATIVE));
    write_object_as_binary_tuple(writer, prim);
}

/**
 * Read primitive from a stream, which is encoded as a binary tuple.
 *
 * @param reader Reader.
 * @return Value.
 */
primitive read_primitive_from_binary_tuple(protocol::reader &reader) {
    auto tuple_data = reader.read_binary();
    binary_tuple_parser parser(3, tuple_data);

    auto typ = static_cast<ignite_type>(binary_tuple_parser::get_int32(parser.get_next()));
    auto scale = binary_tuple_parser::get_int32(parser.get_next());
    return protocol::read_next_column(parser, typ, scale);
}

/**
 * Unpack compute execution result.
 *
 * @param reader Reader.
 * @return Value.
 */
primitive unpack_compute_result(protocol::reader &reader) {
    // Check whether job returned null value
    if (reader.try_read_nil()) {
        return primitive{nullptr};
    }

    auto type_id = ignite_type(reader.read_int32());
    auto job_id = from_type_id(type_id);

    if (job_id != compute_job_type::NATIVE)
        throw ignite_error("Only native compute results are supported currently");

    return read_primitive_from_binary_tuple(reader);
}

/**
 * Read job state from a stream, which is encoded as a binary tuple.
 *
 * @param reader Reader.
 * @return Value.
 */
job_state read_job_state(protocol::reader &reader) {
    job_state res;

    res.id = reader.read_uuid();
    res.status = job_status(reader.read_int32());

    auto create_time = reader.read_timestamp_opt();
    res.create_time = create_time ? *create_time : ignite_timestamp{};
    res.start_time = reader.read_timestamp_opt();
    res.finish_time = reader.read_timestamp_opt();

    return res;
}

/**
 * Read job state from a stream, which is encoded as a binary tuple.
 *
 * @param reader Reader.
 * @return Value or std::nullopt on nil in stream.
 */
std::optional<job_state> read_job_state_opt(protocol::reader &reader) {
    if (reader.try_read_nil())
        return std::nullopt;

    return read_job_state(reader);
}

/**
 * Write units.
 *
 * @param writer Writer to use.
 * @param units Units to write.
 */
void write_units(protocol::writer &writer, const std::vector<deployment_unit> &units) {
    writer.write(static_cast<int32_t>(units.size()));
    for (const auto &unit : units) {
        detail::arg_check::container_non_empty(unit.get_name(), "Deployment unit name");
        detail::arg_check::container_non_empty(unit.get_version(), "Deployment unit version");

        writer.write(unit.get_name());
        writer.write(unit.get_version());
    }
}

/**
 * Response handler implementation for a job compute.
 */
class response_handler_compute final : public response_handler_adapter<job_execution> {
public:
    // Default
    response_handler_compute() = default;

    /**
     * Constructor.
     *
     * @param compute Compute interface.
     * @param callback Callback.
     * @param skip_schema Indicates whether we should skip schema on read.
     */
    explicit response_handler_compute(
        const std::shared_ptr<compute_impl> &compute, ignite_callback<job_execution> callback, bool skip_schema)
        : response_handler_adapter(std::move(callback))
        , m_skip_schema(skip_schema)
        , m_compute(compute) {}

    /**
     * Set error.
     *
     * @param err Error to set.
     */
    [[nodiscard]] ignite_result<void> set_error(ignite_error err) override {
        auto res = result_of_operation<void>([&]() {
            if (!m_execution) {
                m_callback({std::move(err)});
            } else {
                m_execution->set_error(err);
            }
        });

        m_handling_complete = true;
        return res;
    }

    /**
     * Processes a job execution result, submitting it to job_execution.
     *
     * @return Operation result.
     */
    ignite_result<void> process_job_result() {
        assert(m_result_received);

        this->m_handling_complete = true;
        if (m_read_result.has_error()) {
            m_execution->set_error(m_read_result.error());

            return m_read_result;
        }

        auto handle_res = result_of_operation<void>([&]() {
            m_execution->set_result(m_execution_result);
            m_execution->set_final_state(m_final_state);
        });

        return handle_res;
    }

    /**
     * Handle response.
     *
     * @param msg Message.
     * @param flags Flags.
     */
    [[nodiscard]] ignite_result<void> handle(
        std::shared_ptr<node_connection>, bytes_view msg, std::int32_t flags) override final {
        protocol::reader reader(msg);

        if (!test_flag(flags, protocol::response_flag::NOTIFICATION_FLAG)) {
            std::lock_guard<std::mutex> guard(m_state_mutex);
            auto read_res = result_of_operation<job_execution>([&]() {
                if (m_skip_schema)
                    reader.skip();

                auto id = reader.read_uuid();
                auto node = read_cluster_node(reader);
                m_execution = std::make_shared<job_execution_impl>(id, std::move(node), std::move(m_compute));
                return job_execution{m_execution};
            });

            auto handle_res = result_of_operation<void>([&]() { this->m_callback(std::move(read_res)); });
            if (handle_res.has_error() || !m_result_received) {
                return handle_res;
            }

            return process_job_result();
        }

        std::lock_guard<std::mutex> guard(m_state_mutex);
        m_read_result = result_of_operation<void>([&]() {
            m_execution_result = unpack_compute_result(reader);
            m_final_state = read_job_state(reader);
        });
        m_result_received = true;

        if (!m_execution) {
            return {};
        }

        return process_job_result();
    }

private:
    /** Skip schema flag. */
    const bool m_skip_schema{true};

    /** Compute. */
    std::shared_ptr<compute_impl> m_compute;

    /** Execution. */
    std::shared_ptr<job_execution_impl> m_execution{};

    /** Indicates that we've already got a result notification. */
    bool m_result_received{false};

    /** Execution read result. */
    ignite_result<void> m_read_result{};

    /** Execution result. */
    std::optional<primitive> m_execution_result{};

    /** Final job stat. */
    job_state m_final_state;

    /** State mutex. */
    std::mutex m_state_mutex;
};

void compute_impl::submit_to_nodes(const std::set<cluster_node> &nodes, std::shared_ptr<job_descriptor> descriptor,
    const binary_object &arg, ignite_callback<job_execution> callback) {

    auto writer_func = [&nodes, &descriptor, arg](protocol::writer &writer, auto&) {
        auto nodes_num = std::int32_t(nodes.size());
        writer.write(nodes_num);
        for (const auto &node : nodes) {
            writer.write(node.get_name());
        }
        write_units(writer, descriptor->get_deployment_units());
        writer.write(descriptor->get_job_class_name());

        writer.write(descriptor->get_execution_options().get_priority());
        writer.write(descriptor->get_execution_options().get_max_retries());

        pack_compute_argument(writer, arg);
    };

    auto handler = std::make_shared<response_handler_compute>(shared_from_this(), std::move(callback), false);

    m_connection->perform_request_handler(cluster_connection::static_op(protocol::client_operation::COMPUTE_EXECUTE),
        nullptr, writer_func, std::move(handler));
}

void compute_impl::submit_colocated_async(const colocated_job_target &target,
    std::shared_ptr<job_descriptor> descriptor, const binary_object &arg, ignite_callback<job_execution> callback) {
    auto self = shared_from_this();
    auto conn = m_connection;
    auto on_table_get = [self, target, descriptor, arg, conn, callback](auto &&res) mutable {
        if (res.has_error()) {
            callback({std::move(res.error())});
            return;
        }
        auto &table_opt = res.value();
        if (!table_opt) {
            callback({ignite_error("Table does not exist: '" + target.get_table_name().get_canonical_name() + "'")});
            return;
        }

        auto table = table_impl::from_facade(*table_opt);
        table->template with_proper_schema_async<job_execution>(
            callback, [self, table, key = target.get_key(), descriptor, arg, conn](const schema &sch, auto callback) mutable {
                auto writer_func = [&key, &descriptor, &sch, &table, &arg](protocol::writer &writer, auto&) {
                    writer.write(table->get_id());
                    writer.write(sch.version);
                    write_tuple(writer, sch, key, true);
                    write_units(writer, descriptor->get_deployment_units());
                    writer.write(descriptor->get_job_class_name());

                    writer.write(descriptor->get_execution_options().get_priority());
                    writer.write(descriptor->get_execution_options().get_max_retries());

                    pack_compute_argument(writer, arg);
                };

                auto handler = std::make_shared<response_handler_compute>(self, std::move(callback), true);

                conn->perform_request_handler(
                    cluster_connection::static_op(protocol::client_operation::COMPUTE_EXECUTE_COLOCATED),
                    nullptr, writer_func, std::move(handler));
            });
    };

    m_tables->get_table_async(target.get_table_name(), std::move(on_table_get));
}

void compute_impl::get_state_async(uuid id, ignite_callback<std::optional<job_state>> callback) {
    auto writer_func = [id](protocol::writer &writer, auto&) { writer.write(id); };

    auto reader_func = [](protocol::reader &reader) -> std::optional<job_state> {
        return read_job_state_opt(reader);
    };

    m_connection->perform_request<std::optional<job_state>>(
        protocol::client_operation::COMPUTE_GET_STATUS, writer_func, std::move(reader_func), std::move(callback));
}

void compute_impl::cancel_async(uuid id, ignite_callback<job_execution::operation_result> callback) {
    auto writer_func = [id](protocol::writer &writer, auto&) { writer.write(id); };

    auto reader_func = [](protocol::reader &reader) -> job_execution::operation_result {
        typedef job_execution::operation_result operation_result;
        if (reader.try_read_nil())
            return operation_result::NOT_FOUND;

        return reader.read_bool() ? operation_result::SUCCESS : operation_result::INVALID_STATE;
    };

    m_connection->perform_request<job_execution::operation_result>(
        protocol::client_operation::COMPUTE_CANCEL, writer_func, std::move(reader_func), std::move(callback));
}

void compute_impl::change_priority_async(
    uuid id, std::int32_t priority, ignite_callback<job_execution::operation_result> callback) {
    auto writer_func = [id, priority](protocol::writer &writer, auto&) {
        writer.write(id);
        writer.write(priority);
    };

    auto reader_func = [](protocol::reader &reader) -> job_execution::operation_result {
        typedef job_execution::operation_result operation_result;
        if (reader.try_read_nil())
            return operation_result::NOT_FOUND;

        return reader.read_bool() ? operation_result::SUCCESS : operation_result::INVALID_STATE;
    };

    m_connection->perform_request<job_execution::operation_result>(
        protocol::client_operation::COMPUTE_CHANGE_PRIORITY, writer_func, std::move(reader_func), std::move(callback));
}

} // namespace ignite::detail
