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

#pragma once

#include "ignite/common/end_point.h"

#include <vector>

#include <Python.h>

#include "ssl_config.h"


/**
 * Create a new instance of py_connection python class.
 *
 * @param addresses Addresses.
 * @param schema Schema.
 * @param identity Identity.
 * @param secret Secret.
 * @param page_size Page size.
 * @param timeout Timeout in seconds.
 * @param heartbeat_interval Heartbeat interval in seconds.
 * @param autocommit Autocommit flag.
 * @param ssl_cfg SSL Config.
 * @return A new connection class instance.
 */
PyObject* make_py_connection(std::vector<ignite::end_point> addresses, const char* schema, const char* identity,
    const char* secret, int page_size, int timeout, float heartbeat_interval, bool autocommit, ssl_config &&ssl_cfg);

/**
 * Prepare PyConnection type for registration.
 */
int prepare_py_connection_type();

/**
 * Register PyConnection type within module.
 */
int register_py_connection_type(PyObject* mod);