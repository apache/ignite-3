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

#include <ignite/client/ignite_client.h>
#include <ignite/client/ignite_client_configuration.h>

#include <iostream>

int main() {
    std::cout << "Testing Ignite client library linkage..." << std::endl;

    // Test basic configuration creation
    ignite::ignite_client_configuration cfg{"127.0.0.1:10800"};

    std::cout << "Client library test: OK" << std::endl;
    return 0;
}
