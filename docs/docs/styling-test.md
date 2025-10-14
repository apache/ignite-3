# Styling Test Page

This page tests all the custom styling elements for Apache Ignite 3 documentation.

## Typography

This is regular body text. It should use Open Sans font and have a comfortable reading size.

### Headings Hierarchy

#### Level 4 Heading
##### Level 5 Heading
###### Level 6 Heading

Text with **bold emphasis** and *italic emphasis*. Here's a [link to Apache Ignite](https://ignite.apache.org).

## Lists

Ordered list:
1. First item with some text
2. Second item with more text
3. Third item

Unordered list:
- Item one
- Item two with a longer description that wraps to multiple lines
- Item three

## Code Examples

Inline code: `SELECT * FROM table WHERE id = 1`

Code block:
```java
public class Example {
    public static void main(String[] args) {
        System.out.println("Hello, Apache Ignite 3!");
    }
}
```

## Tables

| Header 1 | Header 2 | Header 3 |
|----------|----------|----------|
| Row 1, Col 1 | Row 1, Col 2 | Row 1, Col 3 |
| Row 2, Col 1 | Row 2, Col 2 | Row 2, Col 3 |
| Row 3, Col 1 | Row 3, Col 2 | Row 3, Col 3 |
| Row 4, Col 1 | Row 4, Col 2 | Row 4, Col 3 |

## Admonitions

:::note
This is a note admonition. It should have a blue left border.
:::

:::tip
This is a tip admonition. It should have a yellow left border.
:::

:::caution
This is a caution admonition. It should have a red left border.
:::

:::warning
This is a warning admonition. It should have a red left border.
:::

## Tabs

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
  <TabItem value="java" label="Java" default>

```java
Ignite ignite = Ignite.builder()
    .address("localhost:10800")
    .build();
```

  </TabItem>
  <TabItem value="csharp" label="C#">

```csharp
var ignite = await IgniteClient.StartAsync(
    new IgniteClientConfiguration
    {
        Address = "localhost:10800"
    });
```

  </TabItem>
  <TabItem value="python" label="Python">

```python
client = Client()
client.connect("localhost", 10800)
```

  </TabItem>
</Tabs>

The active tab should have a red underline.

## Images

![Apache Ignite Logo](/img/logo.svg)
