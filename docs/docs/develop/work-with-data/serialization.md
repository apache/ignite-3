---
id: serialization
title: Object Serialization
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Apache Ignite provides a way to serialize your java objects and types and send the data between servers and clients.

## Native Types

Apache Ignite handles native type serialization automatically. For example, the following Compute job accepts an Integer and returns an Integer:

```java
class IntegerComputeJob implements ComputeJob<Integer, Integer> {
    @Override
    public @Nullable CompletableFuture<Integer> executeAsync(
        JobExecutionContext context, @Nullable Integer arg
    ) {
        return completedFuture(arg - 1);
    }
}
```

Since both the argument and the result are native types and are serialized automatically, you do not need any additional code to handle the serialization:

<Tabs>
<TabItem value="java" label="Java">

```java
try (IgniteClient client = IgniteClient.builder().addresses("address/to/cluster:port").build()) {
Integer result = client.compute().execute(
JobTarget.anyNode(client.clusterNodes()),
JobDescriptor.builder(IntegerComputeJob.class).build(),
1
);
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
using var client = await IgniteClient.StartAsync(
    new IgniteClientConfiguration("address/to/cluster:port"));

IJobExecution<int> jobExec = await client.Compute.SubmitAsync(
    JobTarget.AnyNode(await client.GetClusterNodesAsync()),
    new JobDescriptor<int, int>("org.example.IntegerComputeJob"),
    1);

int result = await jobExec.GetResultAsync();
```

</TabItem>
</Tabs>

## Tuples

Apache Ignite is designed around working with Tuples, and handles tuple serialization automatically. For example, the following Job accepts Tuple and returns Tuple:

```java
class TupleComputeJob implements ComputeJob<Tuple, Tuple> {
    @Override
    public @Nullable CompletableFuture<Tuple> executeAsync(JobExecutionContext context, @Nullable Tuple arg) {
        Tuple resultTuple = Tuple.copy(arg);
        resultTuple.set("col", "new value");

        return completedFuture(resultTuple);
    }
}
```

Since both the argument and the result are tuples, they are serialized automatically, and you do not need to handle serialization:

<Tabs>
<TabItem value="java" label="Java">

```java
try (IgniteClient client = IgniteClient.builder().addresses("address/to/cluster:port").build()) {
Tuple resultTuple = client.compute().execute(
JobTarget.anyNode(client.clusterNodes()),
JobDescriptor.builder(TupleComputeJob.class).build(),
Tuple.create().set("col", "value")
);
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
using var client = await IgniteClient.StartAsync(
    new IgniteClientConfiguration("address/to/cluster:port"));

IJobExecution<IIgniteTuple> jobExec = await client.Compute.SubmitAsync(
    JobTarget.AnyNode(await client.GetClusterNodesAsync()),
    new JobDescriptor<IIgniteTuple, IIgniteTuple>("org.example.TupleComputeJob"),
    new IgniteTuple { ["col"] = "value" });

IIgniteTuple result = await jobExec.GetResultAsync();
```

</TabItem>
</Tabs>

## User Objects

User objects are marshalled automatically in the following way:

- If a custom marshaller is defined, it is used.
- If no marshaller is defined, user Java objects are marshalled to binary tuples.
- If there are nested objects, they are recursively marshalled to tuples.

Below is an example of user objects marshalled with custom logic (using JSON serialization with `ObjectMapper`, but you can do whatever you think is good for your use case).

Let's start with Compute job definition that should be a part of the same deployment unit.

### Server-Side

The code below shows how to handle marshalling on a server, so that it can properly send the data to the clients and receive their responses:

- This is the custom object that we will be using as an argument the job:

  ```java
  class ArgumentCustomServerObject {
      int arg1;
      String arg2;
  }
  ```

- We need to define a marshaller for it using the `ObjectMapper` object:

  ```java
  final ObjectMapper MAPPER = new ObjectMapper();

  class ArgumentCustomServerObjectMarshaller implements Marshaller<ArgumentCustomServerObject, byte[]> {
      @Override
      public byte @Nullable [] marshal(@Nullable ArgumentCustomServerObject object) throws UnsupportedObjectTypeMarshallingException {
          try {
              return MAPPER.writeValueAsBytes(object);
          } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
          }
      }

      @Override
      public @Nullable ArgumentCustomServerObject unmarshal(byte @Nullable [] raw) throws UnsupportedObjectTypeMarshallingException {
          try {
              return MAPPER.readValue(raw, ArgumentCustomServerObject.class);
          } catch (IOException e) {
              throw new RuntimeException(e);
          }
      }
  }
  ```

- Let's also create another object that will be used to store Compute job results, and the corresponding marshaller:

  ```java
  class ResultCustomServerObject {
      int res1;
      String res2;
      long res3;
  }

  class ResultCustomServerObjectMarshaller implements Marshaller<ResultCustomServerObject, byte[]> {
      @Override
      public byte @Nullable [] marshal(@Nullable ResultCustomServerObject object) throws UnsupportedObjectTypeMarshallingException {
          try {
              return MAPPER.writeValueAsBytes(object);
          } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
          }
      }

      @Override
      public @Nullable ResultCustomServerObject unmarshal(byte @Nullable [] raw) throws UnsupportedObjectTypeMarshallingException {
          try {
              return MAPPER.readValue(raw, ResultCustomServerObject.class);
          } catch (IOException e) {
              throw new RuntimeException(e);
          }
      }
  }
  ```

The marshallers above define how to represent corresponding objects as `byte[]`, and how to read these objects from `byte[]`. However, defining these classes does not enable custom serialization, as you need to specify the marshaller to use when serializing objects. In Apache Ignite, this is done by overriding two methods in  Compute job definition to use them as factory methods for marshallers:

The code below provides an example of implementing marshallers in a compute job:

```java
class PojoComputeJob implements ComputeJob<ArgumentCustomServerObject, ResultCustomServerObject> {

    @Override
    public @Nullable CompletableFuture<ResultCustomServerObject> executeAsync(
        JobExecutionContext context,
        @Nullable ArgumentCustomServerObject arg
    ) {
        ResultCustomServerObject res = new ResultCustomServerObject();
        res.res1 = arg.arg1;
        res.res2 = arg.arg2;
        res.res3 = 1;

        return completedFuture(res);
    }

    @Override
    public Marshaller<ArgumentCustomServerObject, byte[]> inputMarshaller() {
        return new ArgumentCustomServerObjectMarshaller();
    }

    @Override
    public Marshaller<ResultCustomServerObject, byte[]> resultMarshaller() {
        return new ResultCustomServerObjectMarshaller();
    }
}
```

With this, the Apache Ignite server will be able to handle marshalling the required objects to sending them to clients, and unmarshalling the client responses.

### Client-Side

On the client side, largely the same code is required to handle the incoming objects and to marshal the response:

- Define the custom object that is used for compute job:

  <Tabs>
  <TabItem value="java" label="Java">

  ```java
  class ArgumentCustomClientObject {
  int arg1;
  String arg2;
  }
  ```

  </TabItem>
  <TabItem value="dotnet" label=".NET">

  ```csharp
  record ArgumentCustomClientObject(int arg1, string arg2);
  ```

  </TabItem>
  </Tabs>

- Define the marshaller for the object:

  <Tabs>
  <TabItem value="java" label="Java">

  ```java
  final ObjectMapper MAPPER = new ObjectMapper();

  class ArgumentCustomClientObjectMarshaller implements Marshaller<ArgumentCustomClientObject, byte[]> {
  @Override
  public byte @Nullable [] marshal(@Nullable ArgumentCustomClientObject object) throws UnsupportedObjectTypeMarshallingException {
  try {
  return MAPPER.writeValueAsBytes(object);
  } catch (JsonProcessingException e) {
  throw new RuntimeException(e);
  }
  }

      @Override
      public @Nullable ArgumentCustomClientObject unmarshal(byte @Nullable [] raw) throws UnsupportedObjectTypeMarshallingException {
          try {
              return MAPPER.readValue(raw, ArgumentCustomClientObject.class);
          } catch (IOException e) {
              throw new RuntimeException(e);
          }
      }
  }
  ```

  </TabItem>
  <TabItem value="dotnet" label=".NET">

  ```csharp
  class MyJsonMarshaller<T> : IMarshaller<T>
  {
      public void Marshal(T obj, IBufferWriter<byte> writer)
      {
          using var utf8JsonWriter = new Utf8JsonWriter(writer);
          JsonSerializer.Serialize(utf8JsonWriter, obj);
      }

      public T Unmarshal(ReadOnlySpan<byte> bytes) =>
          JsonSerializer.Deserialize<T>(bytes)!;
  }
  ```

  </TabItem>
  </Tabs>

- Do the same for the result object:

  <Tabs>
  <TabItem value="java" label="Java">

  ```java
  class ResultCustomClientObject {
  int res1;
  String res2;
  long res3;
  }


  class ResultCustomClientObjectMarshaller implements Marshaller<ResultCustomClientObject, byte[]> {
  @Override
  public byte @Nullable [] marshal(@Nullable ResultCustomClientObject object) throws UnsupportedObjectTypeMarshallingException {
  try {
  return MAPPER.writeValueAsBytes(object);
  } catch (JsonProcessingException e) {
  throw new RuntimeException(e);
  }
  }

      @Override
      public @Nullable ResultCustomClientObject unmarshal(byte @Nullable [] raw) throws UnsupportedObjectTypeMarshallingException {
          try {
              return MAPPER.readValue(raw, ResultCustomClientObject.class);
          } catch (IOException e) {
              throw new RuntimeException(e);
          }
      }
  }

  // ....
  ```

  </TabItem>
  <TabItem value="dotnet" label=".NET">

  ```csharp
  record ResultCustomClientObject(int res1, string res2, long res3);

  // Use the same generic MyJsonMarshaller class (see above) for the result object.
  ```

  </TabItem>
  </Tabs>

Now that all marshallers are defined, you can start working with the custom objects and handle marshalling of arguments and results in your compute jobs:

<Tabs>
<TabItem value="java" label="Java">

```java
try (IgniteClient client = IgniteClient.builder().addresses("address/to/cluster:port").build()) {
// Marshalling example of pojo.
ResultCustomClientObject resultPojo = client.compute().execute(
JobTarget.anyNode(client.clusterNodes()),
JobDescriptor.<ArgumentCustomClientObject, ResultCustomClientObject>builder(PojoComputeJob.class.getName())
.argumentMarshaller(new ArgumentCustomClientObjectMarshaller())
.resultMarshaller(new ResultCustomClientObjectMarshaller())
.build(),
new ArgumentCustomClientObject()
);
}
```

</TabItem>
<TabItem value="dotnet" label=".NET">

```csharp
using var client = await IgniteClient.StartAsync(
new IgniteClientConfiguration("address/to/cluster:port"));

IJobExecution<ResultCustomClientObject> jobExec = await client.Compute.SubmitAsync(
JobTarget.AnyNode(await client.GetClusterNodesAsync()),
new JobDescriptor<ArgumentCustomClientObject, ResultCustomClientObject>("org.example.PojoComputeJob")
{
ArgMarshaller = new MyJsonMarshaller<ArgumentCustomClientObject>(),
ResultMarshaller = new MyJsonMarshaller<ResultCustomClientObject>()
},
new ArgumentCustomClientObject(1, "abc"));

ResultCustomClientObject result = await jobExec.GetResultAsync();
```

</TabItem>
</Tabs>
