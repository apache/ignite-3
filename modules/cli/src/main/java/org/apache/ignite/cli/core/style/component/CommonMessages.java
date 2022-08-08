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

package org.apache.ignite.cli.core.style.component;

import org.apache.ignite.cli.core.style.element.UiElements;

/** Common UI messages. */
public class CommonMessages {
    public static MessageUiComponent CONNECT_OR_USE_CLUSTER_URL_MESSAGE = MessageUiComponent.builder()
            .message("You are not connected to node")
            .hint("Run %s command or use %s option",
                    UiElements.command("connect"), UiElements.option("--cluster-url"))
            .build();

    public static MessageUiComponent CONNECT_OR_USE_NODE_URL_MESSAGE = MessageUiComponent.builder()
            .message("You are not connected to node")
            .hint("Run %s command or use %s  option",
                    UiElements.command("connect"), UiElements.option("--node-url"))
            .build();
}
