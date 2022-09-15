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

package org.apache.ignite.cli.commands.node;

import static org.apache.ignite.cli.commands.OptionsConstants.NODE_URL_DESC;
import static org.apache.ignite.cli.commands.OptionsConstants.NODE_URL_OPTION;
import static org.apache.ignite.cli.commands.OptionsConstants.URL_OPTION_SHORT;

import java.net.URL;
import org.apache.ignite.cli.core.converters.UrlConverter;
import picocli.CommandLine.Option;

/**
 * Mixin class for node URL option.
 */
public class NodeUrlMixin {
    /** Node URL option. */
    @Option(names = {URL_OPTION_SHORT, NODE_URL_OPTION}, description = NODE_URL_DESC, converter = UrlConverter.class)
    private URL nodeUrl;

    public String getNodeUrl() {
        return nodeUrl != null ? nodeUrl.toString() : null;
    }
}
