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

package org.apache.ignite.internal.runner.app.client;

import org.apache.ignite.Ignite;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Test;

/**
 * Tests marshaller, ensures consistent behavior across client and embedded modes.
 */
public class ItThinClientMarshallingTest extends ItAbstractThinClientTest {
    protected Ignite ignite() {
        return client();
    }

    // TODO: Same tests run against client and embedded APIs.
    @SuppressWarnings("resource")
    @Test
    public void testUnmappedPojoFieldsAreRejected() {
        Table table = ignite().tables().table(TABLE_NAME);
        var pojoView = table.recordView(TestPojo2.class);

        var pojo = new TestPojo2();
        pojo.key = 1;
        pojo.val = "val";
        pojo.unmapped = "unmapped";

        pojoView.upsert(null, pojo);
    }

    private static class TestPojo2 {
        public int key;

        public String val;

        public String unmapped;
    }
}
