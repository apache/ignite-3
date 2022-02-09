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

package org.apache.ignite.internal.metastorage.common;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.common.command.IfInfo;

public class StatementInfo implements Serializable {

    @SuppressWarnings("MemberName")
    private final IfInfo iif;
    private final UpdateInfo update;

    public StatementInfo(IfInfo iif) {
        this.iif = iif;
        this.update = null;
    }

    public StatementInfo(UpdateInfo update) {
        this.update = update;
        this.iif = null;
    }

    public boolean isTerminal() {
        return update != null;
    }

    public IfInfo iif() {
        return iif;
    }

    public UpdateInfo update() {
        return update;
    }
}
