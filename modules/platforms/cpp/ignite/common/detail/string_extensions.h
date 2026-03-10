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

#pragma once

#include "ignite_date_time.h"
#include "ignite_time.h"
#include "ignite_timestamp.h"

#include <big_decimal.h>
#include <ignite_date.h>
#include <uuid.h>

// TODO IGNITE-28196 functions should be moved to header files for corresponding types.
namespace ignite::detail {

std::string to_string(const big_decimal &bd) noexcept;

std::string to_string(const uuid& uuid) noexcept;

std::string to_string(const ignite_date& date) noexcept;

std::string to_string(const ignite_time& time) noexcept;

std::string to_string(const ignite_date_time& dt) noexcept;

std::string to_string(const ignite_timestamp& ts) noexcept;

}