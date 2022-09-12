/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <future>
#include <memory>

//#include "ignite/table/table.h"
#include "cluster_connection.h"

namespace ignite::impl
{

/**
 * Table management.
 */
class TablesImpl
{
public:
    // Default
    TablesImpl() = default;
    ~TablesImpl() = default;
    TablesImpl(TablesImpl&&) = default;
    TablesImpl(const TablesImpl&) = default;
    TablesImpl& operator=(TablesImpl&&) = default;
    TablesImpl& operator=(const TablesImpl&) = default;

    /**
     * Constructor.
     *
     * @param connection Connection.
     */
    explicit TablesImpl(std::shared_ptr<ClusterConnection> connection) :
        m_connection(std::move(connection)) { }

//    /**
//     * Gets a table implementation by name.
//     * See Table::getTableAsync() for details.
//     *
//     * @param name Table name.
//     * @return TableImpl with corresponding name.
//     * @throw IgniteError In case of error.
//     */
//    [[nodiscard]]
//    std::future<std::shared_ptr<TableImpl>> getTableImplAsync(const std::string& name);

private:
    /** Cluster connection. */
    std::shared_ptr<ClusterConnection> m_connection;
};

} // namespace ignite::impl
