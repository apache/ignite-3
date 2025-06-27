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

package org.apache.ignite.internal.network;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.network.NetworkAddress;

/**
 * A program that accepts original {@link NetworkAddress}es from the command line, instantiates a {@link StaticNodeFinder}
 * from them, calls {@link NodeFinder#findNodes()} and prints the result to stdout.
 *
 * <p>Input must be one command line argument, which is a comma-separated list of hostname:port pairs.
 */
public class StaticNodeFinderMain {
    /**
     * Runs the program.
     *
     * @param args Exactly one argument is expected. Example: host1:3001,host2:3002
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("Exactly one argument is expected, but got " + List.of(args));
        }

        List<NetworkAddress> originalAddresses = Arrays.stream(args[0].split(","))
                .map(NetworkAddress::from)
                .collect(toList());
        NodeFinder finder = new StaticNodeFinder(originalAddresses);

        Collection<NetworkAddress> result = finder.findNodes();

        System.out.print(result.stream().map(NetworkAddress::toString).collect(joining(",")));
    }
}
