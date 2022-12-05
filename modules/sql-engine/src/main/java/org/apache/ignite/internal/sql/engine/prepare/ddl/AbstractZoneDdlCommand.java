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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

/**
 * Abstract zone ddl command.
 */
public class AbstractZoneDdlCommand implements DdlCommand {
    /** Table zone. */
    private String zoneName;

    /** Schema name where this new zone will be created. */
    private String schemaName;

    public String zoneName() {
        return zoneName;
    }

    public void zoneName(String zoneName) {
        this.zoneName = zoneName;
    }

    public String schemaName() {
        return schemaName;
    }

    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
