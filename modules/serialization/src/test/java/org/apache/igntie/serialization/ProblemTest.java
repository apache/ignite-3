package org.apache.igntie.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.InstanceCreator;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.serialization.JsonObjectSerializer;
import org.apache.ignite.serialization.UserObjectSerializer;
import org.junit.jupiter.api.Test;

public class ProblemTest {
    private static final UserObjectSerializer serializer = new JsonObjectSerializer();

    private static final GenericClass<String> result1 = new GenericClass<>("s", "type");
    private static final byte[] ser1 = serializer.serialize(result1);

    private static final GenericClass<Integer> result2 = new GenericClass<>("s", 1);
    private static final byte[] ser2 = serializer.serialize(result2);

    @Test
    public void test() {
        Job1 job1 = new Job1();
        Job2 job2 = new Job2();

        GenericClass<String> execute1 = job1.execute(null);
        assertEquals(result1, execute1);

        GenericClass<Integer> execute2 = job2.execute(null);
        assertEquals(result2, execute2);
    }

    private static class Job1 implements ComputeJob<GenericClass<String>> {

        @Override
        public GenericClass<String> execute(JobExecutionContext context, Object... args) {
            return serializer.deserialize(ser1, GenericClass.class);
        }
    }

    private static class Job2 implements ComputeJob<GenericClass<Integer>> {

        @Override
        public GenericClass<Integer> execute(JobExecutionContext context, Object... args) {
            return serializer.deserialize(ser2, GenericClass.class);
        }
    }

    private static class GenericDeserializer implements JsonDeserializer<GenericClass<Integer>> {

        @Override
        public GenericClass<Integer> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            return context.deserialize(json, typeOfT);
        }
    }

    private static class GenericInstanceCreator implements InstanceCreator<GenericClass<?>> {

        @Override
        public GenericClass<?> createInstance(Type type) {
            Type[] typeParameters = ((ParameterizedType)type).getActualTypeArguments();
            Type idType = typeParameters[0]; // Id has only one parameterized type T

            return new GenericClass<>();
        }
    }

    private static class GenericClass<T> {
        private T type;
        private String s;

        public GenericClass() {

        }

        public GenericClass(String s, T type) {
            this.type = type;
            this.s = s;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            GenericClass<?> generic = (GenericClass<?>) o;

            if (type != null ? !type.equals(generic.type) : generic.type != null) {
                return false;
            }
            return s != null ? s.equals(generic.s) : generic.s == null;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (s != null ? s.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Generic{" +
                    "type=" + type +
                    ", s='" + s + '\'' +
                    '}';
        }
    }
}
