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

package org.apache.ignite.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class ErrorHandler implements CommandLine.IExecutionExceptionHandler, CommandLine.IParameterExceptionHandler {
    Logger logger = LoggerFactory.getLogger(ErrorHandler.class);

    @Override public int handleExecutionException(Exception ex, CommandLine cmd,
        CommandLine.ParseResult parseResult) throws Exception {
        if (ex instanceof IgniteCLIException)
            cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
        else
            logger.error("", ex);

        return cmd.getExitCodeExceptionMapper() != null
            ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
            : cmd.getCommandSpec().exitCodeOnExecutionException();
    }

    @Override public int handleParseException(CommandLine.ParameterException ex, String[] args) {
        CommandLine cli = ex.getCommandLine();

        CliHelper.initCli(cli);

//        cli.getErr().println(cli.getColorScheme().errorText("ERROR: ") + ex.getMessage() + '\n');

        cli.usage(cli.getOut());

        return cli.getCommandSpec().exitCodeOnInvalidInput();
    }
}
