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

package org.apache.ignite.cli.commands;

import jakarta.inject.Inject;
import org.apache.ignite.cli.sql.SqlConnectionMonitor;
import picocli.CommandLine;

/**
 * Command to connect to database by provided jdbc url.
 */
@CommandLine.Command(name = "database")
public class SelectDatabaseCommand implements Runnable {
    @Inject
    private SqlConnectionMonitor sqlConnectionMonitor;
    @CommandLine.Option(names = {"-db"}, required = true)
    private String[] database;

    /**
     * Working method.
     */
    @Override
    public void run() {
        if (database.length == 1) {
            sqlConnectionMonitor.setManager(database[0]);
        } else {
            System.err.println("Only one database can be selected");
        }
    }
}

