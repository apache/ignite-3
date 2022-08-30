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

package org.apache.ignite.cli.core.repl.context;

import java.io.PrintWriter;
import java.util.function.Consumer;
import picocli.CommandLine;

/**
 * Provider of {@link CommandLineContext}.
 */
//Tech Debt: IGNITE-17484
public class CommandLineContextProvider {

    private static volatile CommandLine cmd;

    private static volatile Consumer<Runnable> printWrapper = Runnable::run;

    /**
     * Getter for {@link CommandLineContext}.
     *
     * @return context instance.
     */
    public static CommandLineContext getContext() {
        return new CommandLineContext() {
            @Override
            public PrintWriter out() {
                return cmd.getOut();
            }

            @Override
            public PrintWriter err() {
                return cmd.getErr();
            }
        };
    }

    public static void setCmd(CommandLine cmd) {
        CommandLineContextProvider.cmd = cmd;
    }

    /**
     * Sets the wrapper which will be used when printing from the flow.
     *
     * @param printWrapper print wrapper.
     */
    public static void setPrintWrapper(Consumer<Runnable> printWrapper) {
        CommandLineContextProvider.printWrapper = printWrapper;
    }

    /**
     * Passes the @{link Runnable} to the print wrapper.
     *
     * @param printer lambda which will be wrapped.
     */
    public static void print(Runnable printer) {
        printWrapper.accept(printer);
    }
}
