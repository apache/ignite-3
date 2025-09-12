package org.apache.ignite.example.table;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.TypeConverter;

public class MapperExample {
    static class BinaryTypeConverter implements TypeConverter<Person, byte[]> {

        @Override
        public Person toObjectType(byte[] bytes) throws IOException, ClassNotFoundException {
            try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                return (Person) in.readObject();
            }
        }

        @Override
        public byte[] toColumnType(Person person) throws IOException {
            try (var bos = new ByteArrayOutputStream();
                    var out = new ObjectOutputStream(bos)) {
                out.writeObject(person);
                return bos.toByteArray();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        var mapper = Mapper.builder(Person.class)
                .automap()
                .map("city", "city", new BinaryTypeConverter())
                .build();

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()
        ) {
            RecordView<Person> view = client.tables()
                    .table("person")
                    .recordView(mapper);
        }
    }
}
