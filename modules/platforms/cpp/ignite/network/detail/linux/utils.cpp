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

#include "../utils.h"

#include <cstring>

namespace ignite::network::detail {

#if defined(__linux__)
std::string get_last_system_error() {
    int error_code = errno;

    std::string error_details;
    if (error_code != 0) {
        char err_buf[1024] = {0};

        const char *res = strerror_r(error_code, err_buf, sizeof(err_buf));
        if (res)
            error_details.assign(res);
    }

    return error_details;
}
#elif defined(__APPLE__)
std::string get_last_system_error() {
    int error_code = errno;

    std::string error_details;
    if (error_code != 0) {
        char err_buf[1024] = {0};

        const int res = strerror_r(error_code, err_buf, sizeof(err_buf));

        switch (res) {
            case 0:
                error_details.assign(err_buf);
                break;
            case ERANGE:
                // Buffer too small.
                break;
            default:
            case EINVAL:
                // Invalid error code.
                break;
        }
    }

    return error_details;
}
#endif

} // namespace ignite::network::detail
