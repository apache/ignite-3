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

#include <string>

#include "common_types.h"
#include <ignite/common/expected.h>

namespace ignite
{

/**
 * ODBC error.
 */
class OdbcError
{
public:
    /**
     * Constructor.
     *
     * @param status SQL status.
     * @param message Error message.
     */
    OdbcError(sql_state status, const std::string& message) :
        status(status),
        errMessage(message)
    {
        // No-op.
    }

    /**
     * Default constructor.
     */
    OdbcError() :
        status(sql_state::UNKNOWN),
        errMessage()
    {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    OdbcError(const OdbcError& other) :
        status(other.status),
        errMessage(other.errMessage)
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    ~OdbcError()
    {
        // No-op.
    }

    /**
     * Get status.
     * @return Status.
     */
    sql_state GetStatus() const
    {
        return status;
    }

    /**
     * Get error message.
     * @return Error message.
     */
    const std::string& GetErrorMessage() const
    {
        return errMessage;
    }

private:
    /** Status. */
    sql_state status;

    /** Error message. */
    std::string errMessage;
};

typedef Unexpected<OdbcError> OdbcUnexpected;

/**
 * Expected specialization for OdbcError.
 */
template<typename R>
struct OdbcExpected : Expected<R, OdbcError>
{
    OdbcExpected(const R& res)
        : Expected<R, OdbcError>(res)
    {
        // No-op.
    }

    OdbcExpected(const OdbcError& err)
        : Expected<R, OdbcError>(OdbcUnexpected(err))
    {
        // No-op.
    }
};

} // namespace ignite
