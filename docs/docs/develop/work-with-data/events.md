---
id: events
title: Working with Events
---

Apache Ignite can generate events for a variety of operations happening in the cluster and notify your application about those operations. There are many types of events, including cache events, node discovery events, distributed task execution events, and many more.

## Enabling Events

In Ignite 3, events are configured cluster-wide, in [cluster configuration](/3.1.0/configure-and-operate/configuration/config-cluster-and-nodes). Events are organized in **channels**, each channel tracking one or more event types. You cannot enable or disable individual events, instead you need to disable event channels.

To create an event channel:

```shell
cluster config update ignite.eventlog.channels.exampleChannel.events=["USER_AUTHENTICATION_SUCCESS"]
```

This channel will track the `USER_AUTHENTICATION_SUCCESS`, but not trigger yet. For the events to trigger, a **sink** must be configured. It sends the event information to the configured logger category at the configured level. Currently, only the `log` sink type is supported, and it writes output to Apache Ignite log. Here is how you can enable log sink by using the CLI tool:

```shell
cluster config update ignite.eventlog.sinks.exampleSink = {type="log", channel="exampleChannel"}
```

Now, the authorization events will be written to the log. Here is how the event may look like:

```
2024-06-04 16:19:29:840 +0300 [INFO][%defaultNode%sql-execution-pool-1][EventLog] {"type":"USER_AUTHORIZATION_SUCCESS","timestamp":1717507169840,"productVersion":"3.0.0","user":{"username":"ignite","authenticationProvider":"basic"},"fields":{"privileges":[{"action":"CREATE_TABLE","on":{"objectType":"TABLE","objectName":"TEST2","schema":"PUBLIC"}}],"roles":["system"]}}
```

Below is the cluster configuration config in JSON.

:::note
In Apache Ignite 3, you can create and maintain the configuration in either JSON or HOCON format.
:::

```json
{
  "ignite" : {
    "eventlog" : {
        "channels" : [ {
          "enabled" : true,
          "events" : [ "USER_AUTHENTICATION_SUCCESS" ],
          "name" : "exampleChannel"
        } ],
        "sinks" : [ {
          "channel" : "exampleChannel",
          "criteria" : "EventLog",
          "format" : "JSON",
          "level" : "INFO",
          "name" : "sampleSink",
          "type" : "log"
        } ]
    }
  }
}
```

## Sink Structure

Data sink configuration in Apache Ignite 3 has the following structure:

```json
{
  "channel" : "exampleChannel",
  "criteria" : "EventLog",
  "format" : "JSON",
  "level" : "INFO",
  "name" : "sampleSink",
  "type" : "log"
}
```

| Field | Description |
|-------|-------------|
| channel | The name of the event channel the data sink logs data for. |
| criteria | Logging criteria. By default, only EventLog messages are logged. |
| format | Output format. Currently, only `JSON` messages are supported. |
| level | The level the messages are posted to the log at. Supported values: `ALL`, `TRACE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, `OFF`. Default value: `INFO`. |
| name | Arbitrary sink name. |
| type | Type of event sink. Currently, only `log` sink is supported, and is used to write events to log. |

## Channel Structure

Event channel configuration in Apache Ignite 3 has the following structure:

```json
{
  "enabled" : true,
  "events" : [ "USER_AUTHENTICATION_SUCCESS" ],
  "name" : "exampleChannel"
}
```

| Field | Description |
|-------|-------------|
| enabled | Defines if this event channel is enabled. |
| events | The list of events tracked by the event channel. For the full list of event types, see [Events List](./events-list). |
| name | Arbitrary channel name. |

## Event Structure

All events in Apache Ignite 3 follow the same basic structure described below. Some events provide additional context in the `data` field.

```json
{
  "type": "AUTHENTICATION",
  "user": { "username": "John", "authenticationProvider": "basic" },
  "timestamp": 1715169617,
  "productVersion": "3.0.0",
  "fields": {}
}
```

| Field | Description |
|-------|-------------|
| type | The type of the event. For the full list of event types, see [Events List](./events-list). |
| user | The name of the user, and the [authentication](/3.1.0/configure-and-operate/configuration/config-authentication) provider used to authorize. |
| timestamp | Even time in UNIX epoch time. |
| productVersion | Apache Ignite version used by the client. |
| fields | Event-specific data. |
