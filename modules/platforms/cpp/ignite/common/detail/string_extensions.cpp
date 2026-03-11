// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "string_extensions.h"

#include <sstream>
#include <iomanip>

namespace ignite::detail {

std::string to_string(const big_decimal &bd) {
    std::stringstream ss;
    ss << bd;

    return ss.str();
}

std::string to_string(const uuid& uuid) {
    std::stringstream ss;

    ss << uuid;

    return ss.str();
}

std::string to_string(const ignite_date &date) {
    std::stringstream ss;

    ss << date.get_year()
       << '-'
       << std::setfill('0') << std::setw(2) << std::to_string(date.get_month())
       << '-'
       << std::setfill('0') << std::setw(2) << std::to_string(date.get_day_of_month());

    return ss.str();
}

std::string to_string(const ignite_time &time) {
    std::stringstream ss;

    ss << std::setfill('0') << std::setw(2) << std::to_string(time.get_hour())
       << ':'
       << std::setfill('0') << std::setw(2) << std::to_string(time.get_minute())
       << ':'
       << std::setfill('0') << std::setw(2) << std::to_string(time.get_second())
       << '.'
       << std::setfill('0') << std::setw(9) << std::to_string(time.get_nano());

    return ss.str();
}

std::string to_string(const ignite_date_time &dt) {
    std::stringstream ss;

    ss << to_string(dt.date()) << 'Z' << to_string(dt.time());

    return ss.str();
}

std::string to_string(const ignite_timestamp &ts) {
    std::stringstream ss;

    ss << std::to_string(ts.get_epoch_second())
       << '.'
       << std::setfill('0') << std::setw(9) << std::to_string(ts.get_nano());

    return ss.str();
}

} // namespace ignite::detail