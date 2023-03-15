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

package org.apache.ignite.internal.cli.core.repl.context;

import java.io.PrintWriter;
import java.util.function.Consumer;
import picocli.CommandLine;

/**
 * Provider of {@link CommandLineContext}.
 */
public class CommandLineContextProvider {

    private static volatile CommandLineContext context;

    private static volatile Consumer<Runnable> printWrapper = Runnable::run;

    /**
     * Getter for {@link CommandLineContext}.
     *
     * @return context instance.
     */
    public static CommandLineContext getContext() {
        return context;
    }

    /**
     * Sets a context from {@link CommandLine} instance.
     *
     * @param cmd {@link CommandLine} instance
     */
    public static void setCmd(CommandLine cmd) {
        context = new CommandLineContext() {
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

    /**
     * Sets a context from {@link PrintWriter}.
     *
     * @param out output writer
     * @param err error output writer
     */
    public static void setWriters(PrintWriter out, PrintWriter err) {
        context = new CommandLineContext() {
            @Override
            public PrintWriter out() {
                return out;
            }

            @Override
            public PrintWriter err() {
                return err;
            }
        };
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
