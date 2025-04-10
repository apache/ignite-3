/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.cli.mixins;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import org.apache.ignite.migrationtools.sql.SqlDdlGenerator;
import picocli.CommandLine;

/** Option that allows the user to add 3rd party libraries to the command custom classloader. */
public class ClassloaderOption {
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    private ClassLoader clientClassLoader;

    /**
     * Parses the command line argument and create the classloader field.
     *
     * @param jarFilePaths Cmd line argument.
     */
    @CommandLine.Option(
            paramLabel = "<jarArchive>",
            names = {"--extra-lib"},
            description = "Jar files to add to the classpath. To add multiple jars, specify the option multiple times.")
    public void classloader(Path[] jarFilePaths) {
        URL[] libraries;
        if (jarFilePaths != null) {
            libraries = Arrays.stream(jarFilePaths)
                    .map(p -> {
                        if (!p.getFileName().toString().endsWith(".jar") || !Files.isRegularFile(p)) {
                            throw new CommandLine.ParameterException(spec.commandLine(),
                                    String.format("Invalid value '%s' for option '--extra-lib': path does not point to a jar archive.", p));
                        }

                        try {
                            return p.toUri().toURL();
                        } catch (MalformedURLException e) {
                            throw new CommandLine.ParameterException(spec.commandLine(),
                                    String.format("Invalid value '%s' for option '--extra-lib': path could not be parsed.", p), e);
                        }
                    })
                    .toArray(URL[]::new);
        } else {
            libraries = new URL[0];
        }

        this.clientClassLoader = AccessController.doPrivileged(
                (PrivilegedAction<URLClassLoader>) () -> new URLClassLoader(libraries, SqlDdlGenerator.class.getClassLoader()));
    }

    public ClassLoader clientClassLoader() {
        return clientClassLoader;
    }
}
