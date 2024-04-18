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

/**
 * MBean implementation, which produce JMX API representation for {@link MetricSet}.
 * Every {@link Metric} of metric set will be represented by MBean's attribute with the same name.
 */
public class MetricSetMbean implements DynamicMBean {
    /**
     * Metric set.
     */
    private MetricSet metricSet;

    /**
     * Constructs new MBean.
     *
     * @param metricSet Metric set.
     */
    public MetricSetMbean(MetricSet metricSet) {
        this.metricSet = metricSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException {
        if (attribute.equals("MBeanInfo")) {
            return getMBeanInfo();
        }

        Metric metric = metricSet.get(attribute);

        if (metric instanceof DoubleMetric) {
            return ((DoubleMetric) metric).value();
        } else if (metric instanceof IntMetric) {
            return ((IntMetric) metric).value();
        } else if (metric instanceof LongMetric) {
            return ((LongMetric) metric).value();
        } else if (metric instanceof DistributionMetric) {
            return ((DistributionMetric) metric).value();
        }

        throw new AttributeNotFoundException("Unknown metric class " + metric.getClass());
    }

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
        if ("getAttribute".equals(actionName)) {
            try {
                return getAttribute((String) params[0]);
            } catch (AttributeNotFoundException e) {
                throw new MBeanException(e);
            }
        }

        throw new UnsupportedOperationException("invoke is not supported.");
    }

    /**
     * {@inheritDoc}
     */
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
                attrs.toArray(new MBeanAttributeInfo[0]),
                null,
                null,
                null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAttribute(Attribute attribute) {
        throw new UnsupportedOperationException("setAttribute is not supported.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException("setAttributes is not supported.");
    }

    /**
     * Extract class of metric value.
     *
     * @param metric Metric.
     * @return Class of metric value.
     */
    private String metricClass(Metric metric) {
        if (metric instanceof DoubleMetric) {
            return Double.class.getName();
        } else if (metric instanceof IntMetric) {
            return Integer.class.getName();
        } else if (metric instanceof LongMetric) {
            return Long.class.getName();
        } else if (metric instanceof DistributionMetric) {
            return long[].class.getName();
        }

        throw new IllegalArgumentException("Unknown metric class " + metric.getClass());
    }
}
