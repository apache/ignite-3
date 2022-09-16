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

#include <boost/test/unit_test.hpp>

#include "common/Uuid.h"

#include <sstream>

BOOST_AUTO_TEST_CASE(construct) {
    {
        ignite::Uuid uuid;
        BOOST_CHECK_EQUAL(0, uuid.getMostSignificantBits());
        BOOST_CHECK_EQUAL(0, uuid.getLeastSignificantBits());
    }
    {
        ignite::Uuid uuid(1, 2);
        BOOST_CHECK_EQUAL(1, uuid.getMostSignificantBits());
        BOOST_CHECK_EQUAL(2, uuid.getLeastSignificantBits());
    }
    {
        ignite::Uuid uuid(1, 2);
        ignite::Uuid uuid2(uuid);
        BOOST_CHECK_EQUAL(1, uuid2.getMostSignificantBits());
        BOOST_CHECK_EQUAL(2, uuid2.getLeastSignificantBits());
        BOOST_CHECK_EQUAL(uuid, uuid2);
    }
}

BOOST_AUTO_TEST_CASE(stream) {
    std::string uuidString = "123e4567-e89b-12d3-a456-426614174000";

    std::stringstream stream;
    stream << uuidString;

    ignite::Uuid uuid;
    stream >> uuid;

    BOOST_CHECK_EQUAL(0x123e4567e89b12d3, uuid.getMostSignificantBits());
    BOOST_CHECK_EQUAL(0xa456426614174000, uuid.getLeastSignificantBits());

    BOOST_CHECK_EQUAL(1, uuid.version());
    BOOST_CHECK_EQUAL(2, uuid.variant());

    std::stringstream stream2;
    stream2 << uuid;

    std::string uuidString2;
    stream2 >> uuidString2;

    BOOST_CHECK_EQUAL(uuidString, uuidString2);
}
