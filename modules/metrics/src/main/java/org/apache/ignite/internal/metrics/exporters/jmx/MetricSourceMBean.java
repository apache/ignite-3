package org.apache.ignite.internal.metrics.exporters.jmx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import org.apache.ignite.internal.metrics.DistributionMetric;
import org.apache.ignite.internal.metrics.DoubleMetric;
import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSet;

public class MetricSourceMBean implements DynamicMBean {
    private MetricSet metricSet;

    public MetricSourceMBean(MetricSet metricSet) {
        this.metricSet = metricSet;
    }

    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException {
        if (attribute.equals("MBeanInfo"))
            return getMBeanInfo();

        Metric metric = metricSet.get(attribute);

        if (metric instanceof DoubleMetric)
            return ((DoubleMetric) metric).value();
        else if (metric instanceof IntMetric)
            return ((IntMetric) metric).value();
        else if (metric instanceof LongMetric)
            return ((LongMetric) metric).value();
        else if (metric instanceof DistributionMetric)
            return ((DistributionMetric) metric).value();

        throw new AttributeNotFoundException("Unknown metric class " + metric.getClass());
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();

        try {
            for (String attribute : attributes) {
                Object val = getAttribute(attribute);

                list.add(val);
            }
        } catch (AttributeNotFoundException e) {
            throw new IllegalArgumentException(e);
        }

        return list;
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
        if ("getAttribute".equals(actionName)) {
            try {
                return getAttribute((String)params[0]);
            } catch (AttributeNotFoundException e) {
                throw new MBeanException(e);
            }
        }
        else if ("invoke".equals(actionName))
            return invoke((String)params[0], (Object[])params[1], (String[])params[2]);

        throw new UnsupportedOperationException("invoke is not supported.");
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        Iterator<Metric> iter = metricSet.iterator();

        List<MBeanAttributeInfo> attrs = new ArrayList<>();

        iter.forEachRemaining(metric -> {
                attrs.add(new MBeanAttributeInfo(
                        metric.name(),
                        metricClass(metric),
                        metric.description() != null ? metric.description() : metric.name(),
                        true,
                        false,
                        false));
        });

        return new MBeanInfo(
                MetricManager.class.getName(),
                metricSet.name(),
                attrs.toArray(new MBeanAttributeInfo[attrs.size()]),
                null,
                null,
                null);
    }

    private String metricClass(Metric metric) {
        if (metric instanceof DoubleMetric)
            return Double.class.getName();
        else if (metric instanceof IntMetric)
            return Integer.class.getName();
        else if (metric instanceof LongMetric)
            return Long.class.getName();
        else if (metric instanceof DistributionMetric)
            return long[].class.getName();

        throw new IllegalArgumentException("Unknown metric class. " + metric.getClass());
    }

    @Override
    public void setAttribute(Attribute attribute) {
        throw new UnsupportedOperationException("setAttribute is not supported.");
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes is not supported.");
    }
}
