# Ignite Event Log

Event log is a feature that allows to log events that happen in the system into some destination.

There are several things that can be configured in this module:

- Sink: The destination where the events will be logged. The sink can be a file, a database, a message queue, etc. 
Now only logger sink is supported.
- Channels: Group different types of events that can be written into several sinks. 
For example, there can be a channel for EVENT_TYPE_1 and EVENT_TYPE_2. All other events won't be logged into any sink that is
piped to this channel.
- Event Types: The type of the event. Each module can define and fire its own event types.

## For users

There is a finite number of event types that is defined in the system. You can not create new event types. 

To start logging events and then read them, you need to configure the event log.

### Configuration

``` 
eventlog:
  channels.authenticationChannel: {
      enabled: true,
      types: [USER_AUTHENTICATED] 
    }
  sinks.authenticationLoggerSink: {
      type: "log",
      criteria: "authEventLog",
      channel: "authenticationChannel"
    }
```

This configuration defines a channel called `authenticationChannel` that will log events of type `USER_AUTHENTICATED` 
into the sink `authenticationLoggerSink`. The sink has a type `log` that means that the events will be logged into 
the logger that is defined for the system. The criteria is a name of the logger that will be used to log the events.
You can configure the logger with a name `authEventLog` in the logger configuration.

## For developers

To log an event, you need to use the `EventLog` interface. This is the only way to do it. If you want to define your
own event type, you need to register it in the `EventTypeRegistry` before the first creation of the event of this type.
