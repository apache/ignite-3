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

package org.apache.ignite.internal.streamer;

import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Integration test for client-side data streamer API.
 */
public class ItClientDataStreamerTest extends ItAbstractDataStreamerTest {
    private static IgniteClient client;

    @BeforeAll
    public static void startClient() {
        client = IgniteClient.builder().addresses("localhost").build();
    }

    @AfterAll
    public static void stopClient() throws Exception {
        client.close();
    }

    @Override
    Ignite ignite() {
        return client;
    }
}
