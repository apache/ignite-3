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

package org.apache.ignite.internal.app;

import org.apache.ignite.table.Tuple;

/**
 * Simple record for tests.
 */
@SuppressWarnings({"FieldCanBeLocal", "unused"})
class Record {
    private int id;
    private String val;

    private Record() {
    }

    Record(int id, String val) {
        this.id = id;
        this.val = val;
    }

    Tuple toKeyTuple() {
        return Tuple.create().set("id", id);
    }

    Tuple toFullTuple() {
        return Tuple.create().set("id", id).set("val", val);
    }
}
