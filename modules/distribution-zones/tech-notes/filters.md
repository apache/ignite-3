## Introduction

[IEP about Distribution Zones](https://cwiki.apache.org/confluence/display/IGNITE/IEP-101%3A+Distribution+Zones) includes feature for
filtering data nodes. It allows users to specify which nodes should be included to data nodes of a specified zone.
It is implemented in the way that user can specify any number of pairs (key, attribute) for a node through the local configuration 
of a node, and also can specify a filter through the SQL syntax of distribution zones.

## Node's attributes
Using mechanism of specifying a local configuration of a node, it is possible to add attributes to a node, so they could be 
used in a data nodes calculation and could be filtered taking into account this attributes. 

In the `ignite-config.conf` it looks like this:

```
nodeAttributes.nodeAttributes {
      region.attribute = "US"
      storage.attribute = "SSD"
}
```

or like this in a json format:

```
{
   "nodeAttributes":{
      "nodeAttributes":{
         "region":{
            "attribute":"US"
         },
         "storage":{
            "attribute":"SSD"
         }
      }
   }
}
```

## Zone's filter
It is possible to specify a filter for a distribution zone on a zone's creation and altering.

```
CREATE ZONE "TEST_ZONE" WITH "DATA_NODES_FILTER" = '$[?(@.region == "US" && @.storage == "SSD")]'
```
```
ALTER ZONE "TEST_ZONE" SET "DATA_NODES_FILTER " = '$..*'" 
```

To express filter, JSONPath syntax is used. From the technical point of view, to filter data nodes, we just match Json representation of 
a node attribute with the corresponding filter.

Some examples of filters:

* Node attributes: `("region" -> "US", "storage" -> "SSD")`; filter: `$[?(@.region == 'US')]`; result: true
* Node attributes: `("region" -> "US")`; filter: `$[?(@.storage == 'SSD' && @.region == 'US')]`; result: false
* Node attributes: `("region" -> "US")`; filter: `$[?(@.storage == 'SSD']`; result: false
* Node attributes: `("region" -> "US")`; filter: `$[?(@.storage != 'SSD']`; result: true
* Node attributes: `("region" -> "US", "dataRegionSize: 10)`; filter: `$[?(@.region == 'EU' || @.dataRegionSize > 5)]`; result: true

To check all capabilities of JSONPath, check https://github.com/json-path/JsonPath

Note that as a default value for filter '$..*' filter is used, meaning that all nodes are match the filter.



