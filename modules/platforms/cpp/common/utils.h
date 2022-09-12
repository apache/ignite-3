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
#include <utility>

#include <common/ignite_error.h>

namespace ignite
{

/**
 * Make future error.
 *
 * @tparam T Future type.
 * @param err Error.
 * @return Failed future with the specified error.
 */
template<typename T>
std::future<T> makeFutureError(IgniteError err)
{
    std::promise<T> promise;
    promise.set_exception(std::make_exception_ptr(std::move(err)));

    return promise.get_future();
}

} // namespace ignite
