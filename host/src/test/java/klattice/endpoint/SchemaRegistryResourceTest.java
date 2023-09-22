package klattice.endpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import klattice.schema.SchemaEntry;
import klattice.schema.SchemaRegistryResource;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
public class SchemaRegistryResourceTest {
    private static final String EX_SCHEMA = """
            {
               "type" : "record",
               "namespace" : "Tutorialspoint",
               "name" : "Employee",
               "fields" : [
                  { "name" : "Name" , "type" : "string" },
                  { "name" : "Age" , "type" : "int" }
               ]
            }
            """;
    @RestClient
    SchemaRegistryResource schemaRegistryResource;

    @Test
    public void test_Types() {
        var types = schemaRegistryResource.types();
        assertArrayEquals(new String[] { "JSON", "PROTOBUF", "AVRO" }, types.toArray());
    }

    @Test
    public void test_SchemaList() {
        schemaRegistryResource.add("topic.a", new SchemaEntry(EX_SCHEMA, "AVRO"));
        var schemaSubjects = schemaRegistryResource.allSubjectsByPrefix("topic.");
        assertEquals(1, schemaSubjects.size());
        assertEquals("topic.a", schemaSubjects.get(0));
    }

    @Test
    public void test_SubjectRetrieval() throws JsonProcessingException {
        schemaRegistryResource.add("topic.a", new SchemaEntry(EX_SCHEMA, "AVRO"));
        var s1 = schemaRegistryResource.byTopicName("topic.a");
        var s2 = schemaRegistryResource.byId(s1.version());
        assertEquals("topic.a", s1.subject());
        assertEquals("AVRO", s1.schemaTypeStr());
        assertEquals("AVRO", s2.schemaTypeStr());
        var objectMapper = new ObjectMapper();
        assertEquals(objectMapper.readTree(EX_SCHEMA), objectMapper.readTree(s1.schema()));
        assertEquals(objectMapper.readTree(EX_SCHEMA), objectMapper.readTree(s2.schema()));
    }
}
