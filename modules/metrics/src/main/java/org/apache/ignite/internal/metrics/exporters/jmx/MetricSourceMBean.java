package org.apache.ignite.internal.metrics.exporters.jmx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import org.apache.ignite.internal.metrics.CompositeMetric;
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
    public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
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

        throw new IllegalArgumentException("Unknown metric class. " + metric.getClass());
    }


    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList();

        try {
            for (String attribute : attributes) {
                Object val = getAttribute(attribute);

                list.add(val);
            }

            return list;
        }
        catch (MBeanException | ReflectionException | AttributeNotFoundException e) {
            // TODO: KKK choose right exception
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
        try {
            if ("getAttribute".equals(actionName))
                return getAttribute((String)params[0]);
            else if ("invoke".equals(actionName))
                return invoke((String)params[0], (Object[])params[1], (String[])params[2]);
        }
        catch (AttributeNotFoundException e) {
            throw new MBeanException(e);
        }

        throw new UnsupportedOperationException("invoke is not supported.");
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        Iterator<Metric> iter = metricSet.iterator();

        List<MBeanAttributeInfo> attributes = new ArrayList<>();

        iter.forEachRemaining(metric -> {
                attributes.add(new MBeanAttributeInfo(
                        metric.name(),
                        metricClass(metric),
                        metric.description() != null ? metric.description() : metric.name(),
                        true,
                        false,
                        false));
        });

        return new MBeanInfo(
                // TODO: KKK not sure, that it is correct. ignite2 use ReadOnlyMetricManager here
                MetricManager.class.getName(),
                metricSet.name(),
                attributes.toArray(new MBeanAttributeInfo[attributes.size()]),
                null,
                null,
                null);
    }

    private String metricClass(Metric metric) {
        if (metric instanceof DoubleMetric)
            return DoubleMetric.class.getName();
        else if (metric instanceof IntMetric)
            return IntMetric.class.getName();
        else if (metric instanceof LongMetric)
            return LongMetric.class.getName();
        else if (metric instanceof DistributionMetric)
            // TODO: KKK suport composite metrics
            return DistributionMetric.class.getName();

        throw new IllegalArgumentException("Unknown metric class. " + metric.getClass());
    }

    @Override
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new UnsupportedOperationException("setAttribute is not supported.");
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes is not supported.");
    }
}
