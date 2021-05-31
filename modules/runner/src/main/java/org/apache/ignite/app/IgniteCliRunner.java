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

package org.apache.ignite.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.app.IgnitionImpl;

/**
 * Sample application integrating new configuration module and providing standard REST API to access and modify it.
 */
public class IgniteCliRunner {
    /**
     * Main entry point for run Ignite node.
     *
     * @param args CLI args to start new node.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Args parsedArgs = null;

        try {
            parsedArgs = Args.parseArgs(args);
        }
        catch (Args.ParseException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }

        String jsonCfgStr = null;
        if (parsedArgs.config != null)
            jsonCfgStr = Files.readString(parsedArgs.config);

        var ignition = new IgnitionImpl();

        ignition.start(parsedArgs.nodeName, jsonCfgStr);
    }

    /**
     * Simple value object with needed CLI args of ignite runner.
     */
    private static class Args {
        /** CLI usage message. */
        private static String usage = "IgniteCliRunner [--config conf] nodeName";

        /** Name of the node. */
        public final String nodeName;

        /** Path to config file. */
        public final Path config;

        public Args(String nodeName, Path config) {
            this.nodeName = nodeName;
            this.config = config;
        }

        /**
         * Simple CLI arguments parser.
         *
         * @param args CLI arguments.
         * @return Parsed arguments.
         * @throws ParseException if required args are absent.
         */
        private static Args parseArgs(String[] args) throws ParseException {
            if (args.length == 1)
                return new Args(args[0], null);
            else if (args.length == 3) {
                if ("--config".equals(args[0]))
                    return new Args(args[2], Path.of(args[1]));
                else
                    throw new ParseException(usage);
            }
            else
                throw new ParseException(usage);
        }

        /**
         * Exception for indicate any problems with parsing CLI args.
         */
        private static class ParseException extends Exception {

            public ParseException(String message) {
                super(message);
            }
        }
    }
}
