package org.apache.ignite.cli;

import io.micronaut.context.ApplicationContext;
import org.apache.ignite.cli.spec.IgniteCliSpec;

public class IgniteCliApp {
    public static void main(String... args) {
        ApplicationContext applicationContext = ApplicationContext.run();

        System.exit(IgniteCliSpec.initCli(applicationContext).execute(args));
    }

}
