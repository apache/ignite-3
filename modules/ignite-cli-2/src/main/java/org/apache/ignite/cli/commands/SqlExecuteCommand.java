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
import java.sql.SQLException;
import org.apache.ignite.cli.sql.MonitorNotInitializedException;
import org.apache.ignite.cli.sql.SqlConnectionMonitor;
import org.apache.ignite.cli.sql.SqlManager;
import picocli.CommandLine;

/**
 * Command to execute provided sql command in connected database.
 */
@CommandLine.Command(name = "execute", mixinStandardHelpOptions = true,
        description = {"Execute sql command in specified database"})
public class SqlExecuteCommand implements Runnable {
    @Inject
    private SqlConnectionMonitor sqlConnectionMonitor;
    @CommandLine.Option(names = {"-sql"})
    private String[] sqlCommand;

    /**
     * Working method.
     */
    @Override
    public void run() {
        if (sqlCommand.length == 1) {
            try {
                SqlManager currentManager = sqlConnectionMonitor.getCurrentManager();
                currentManager.executeSql(sqlCommand[0]);
            } catch (SQLException e) {
                System.err.println("Problem with executing sql " + e.getMessage());
            } catch (MonitorNotInitializedException me) {
                System.err.println("Database not selected");
            }
        }
    }
}
