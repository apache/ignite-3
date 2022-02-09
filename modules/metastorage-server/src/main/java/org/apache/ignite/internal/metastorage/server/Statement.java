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

package org.apache.ignite.internal.metastorage.server;

public class Statement {
    private final If iif;
    private final Update update;

    public Statement(If iif) {
        this.iif = iif;
        this.update = null;
    }

    public Statement(Update update) {
        this.update = update;
        this.iif = null;
    }

    public boolean isTerminal() {
        return update != null;
    }

    public If iif() {
        return iif;
    }

    public Update update() {
        return update;
    }
}
