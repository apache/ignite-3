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

#ifdef _WIN32
# include "detail/win_process.h"
#else
# include "detail/unix_process.h"
#endif

#include "cmd_process.h"

#include <filesystem>
#include <utility>
#include <vector>

namespace ignite {

std::unique_ptr<CmdProcess> CmdProcess::make(std::string command, std::vector<std::string> args, std::string workDir) {
#ifdef _WIN32
    return std::unique_ptr<CmdProcess>(new detail::WinProcess(std::move(command), std::move(args), std::move(workDir)));
#else
    return std::unique_ptr<CmdProcess>(
        new detail::UnixProcess(std::move(command), std::move(args), std::move(workDir)));
#endif
}

} // namespace ignite
