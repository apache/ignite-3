package org.apache.ignite.compute;

import java.util.List;

public class TaskDescriptor {
    private final String taskClassName;

    private final List<DeploymentUnit> units;

    public TaskDescriptor(String taskClassName) {
        this(taskClassName, List.of());
    }

    public TaskDescriptor(String taskClassName, List<DeploymentUnit> units) {
        this.taskClassName = taskClassName;
        this.units = units;
    }

    public String taskClassName() {
        return taskClassName;
    }

    public List<DeploymentUnit> units() {
        return units;
    }
}
