package org.apache.ignite.configuration.processor.internal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.tools.JavaFileObject;
import org.apache.commons.io.IOUtils;
import spoon.Launcher;

public class ConfigSet {

    private final JavaFileObject configurationClass;
    private final JavaFileObject viewClass;
    private final JavaFileObject initClass;
    private final JavaFileObject changeClass;

    private final ParsedClass conf;
    private final ParsedClass view;
    private final ParsedClass init;
    private final ParsedClass change;

    public ConfigSet(JavaFileObject configurationClass, JavaFileObject viewClass, JavaFileObject initClass, JavaFileObject changeClass) {
        this.configurationClass = configurationClass;
        this.viewClass = viewClass;
        this.initClass = initClass;
        this.changeClass = changeClass;

        if (configurationClass != null)
            this.conf = parse(configurationClass);
        else
            this.conf = null;

        if (viewClass != null)
            this.view = parse(viewClass);
        else
            this.view = null;

        if (initClass != null)
            this.init = parse(initClass);
        else
            this.init = null;

        if (changeClass != null)
            this.change = parse(changeClass);
        else
            this.change = null;
    }

    private ParsedClass parse(JavaFileObject clz) {
        String classFileContent;
        try {
            classFileContent = IOUtils.toString(clz.openInputStream(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse class: " + e.getMessage(), e);
        }

        return new ParsedClass(Launcher.parseClass(classFileContent));
    }

    public boolean allGenerated() {
        return configurationClass != null && viewClass != null && initClass != null && changeClass != null;
    }

    public ParsedClass getConfigurationClass() {
        return conf;
    }

    public ParsedClass getViewClass() {
        return view;
    }

    public ParsedClass getInitClass() {
        return init;
    }

    public ParsedClass getChangeClass() {
        return change;
    }
}
