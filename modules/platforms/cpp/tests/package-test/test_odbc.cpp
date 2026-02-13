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

#ifdef WIN32
# include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <iostream>

int main() {
    std::cout << "Testing Ignite ODBC library linkage..." << std::endl;

    SQLHENV env = nullptr;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

    if (env) {
        SQLFreeHandle(SQL_HANDLE_ENV, env);
    }

    std::cout << "ODBC library test: OK (return code: " << ret << ")" << std::endl;
    return 0;
}
