## Introduction

[IEP about Distribution Zones](https://cwiki.apache.org/confluence/display/IGNITE/IEP-101%3A+Distribution+Zones) includes the feature for
filtering data nodes. It allows users to specify which nodes should be included to data nodes of a specified zone.
It is implemented in the way that user can specify any number of pairs (key, attribute) for a node through the local configuration 
of a node, and also can specify a filter through the SQL syntax of distribution zones. Any time data nodes will be recalculated, 
corresponding filter will be applied to set of nodes with pre-defined attributes, so resulting data nodes will be filtered according to 
user's filter.

## Node's attributes
Using mechanism of specifying a local configuration of a node, it is possible to add attributes to a node, so they could be 
used in a data nodes calculation and could be filtered taking into account this attributes. From the technical point of view, 
node's attributes are represented through the JSON mechanism, so all corresponding rules for JSON data are applied to node's attributes.

In the `ignite-config.conf` it looks like this:

```
nodeAttributes.nodeAttributes {
      region.attribute = "US"
      storage.attribute = "SSD"
}
```

or like this in a `ignite-config.json`:

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

Note that node's attributes could be changed only after node's restart.

## Zone's filter
It is possible to specify a filter for a distribution zone on a zone's creation and altering.

```
CREATE ZONE "TEST_ZONE" WITH "DATA_NODES_FILTER" = '$[?(@.region == "US" && @.storage == "SSD")]'
```
```
ALTER ZONE "TEST_ZONE" SET "DATA_NODES_FILTER " = '$..*'" 
```

To express filter, JSONPath syntax is used. From the technical point of view, to filter data nodes, we just match JSON representation of 
a node attribute with the corresponding filter. 

Some examples of filtering results:

* Node attributes are `("region" -> "US", "storage" -> "SSD")`; the filter is `$[?(@.region == 'US')]`; result: node pass the filter and will be included to data nodes
* Node attributes are `("region" -> "US")`; the filter is `$[?(@.storage == 'SSD' && @.region == 'US')]`; result: node do not pass the filter and won't be included to data nodes
* Node attributes are `("region" -> "US")`; the filter is `$[?(@.storage == 'SSD']`; result: node do not pass the filter and won't be included to data nodes
* Node attributes are `("region" -> "US")`; the filter is `$[?(@.storage != 'SSD']`; result: node pass the filter and will be included to data nodes
* Node attributes are `("region" -> "US", "dataRegionSize: 10)`; the filter is `$[?(@.region == 'EU' || @.dataRegionSize > 5)]`; result: node pass the filter and will be included to data nodes

To check all capabilities of JSONPath, see https://github.com/json-path/JsonPath

Note that as a default value for filter '$..*' filter is used, meaning that all nodes match the filter.
Also it is important, that if there are no specified attributes for a node, it means that a node match only the default filter. 



