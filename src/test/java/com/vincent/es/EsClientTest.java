package com.vincent.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
public class EsClientTest {

    @Autowired
    private ElasticsearchClient client;

    private final String INDEX_STUDENT = "student";

    @Before
    public void init() throws IOException {
        var deleteIndexReq = new DeleteIndexRequest.Builder()
                .index(INDEX_STUDENT)
                .build();
        client.indices().delete(deleteIndexReq);

        var createIndexReq = new CreateIndexRequest.Builder()
                .index(INDEX_STUDENT)
                .build();
        client.indices().create(createIndexReq);
    }

    @Test
    public void testCreateDocument() throws IOException {
        var student = SampleData.get().get(0);
        var createRes = createDocument(student);

        assertEquals(student.getId(), createRes.id());
    }

    @Test
    public void testGetDocument() throws IOException {
        var student = SampleData.get().get(0);
        var id = createDocument(student).id();

        var getRes = getDocument(id);
        assertTrue(getRes.found());

        var actualStudent = Optional.ofNullable(getRes.source()).orElseThrow();
        assertEquals(student.getId(), actualStudent.getId());
        assertEquals(student.getName(), actualStudent.getName());
    }

    @Test
    public void testUpdateDocument() throws IOException {
        var student = SampleData.get().get(0);
        createDocument(student);

        student.setName("Vincent Jheng");
        student.setGrade(3);

        var indexReq = new IndexRequest.Builder<Student>()
                .index(INDEX_STUDENT)
                .id(student.getId())
                .document(student)
                .build();
        client.index(indexReq);
        var getRes = getDocument(student.getId());

        var actualStudent = Optional.ofNullable(getRes.source()).orElseThrow();
        assertEquals(student.getId(), actualStudent.getId());
        assertEquals(student.getName(), actualStudent.getName());
        assertEquals(student.getGrade(), actualStudent.getGrade());
        assertEquals(student.getIntroduction(), actualStudent.getIntroduction());
    }

    @Test
    public void testDeleteDocument() throws IOException {
        var student = SampleData.get().get(0);
        createDocument(student);

        var deleteReq = new DeleteRequest.Builder()
                .index(INDEX_STUDENT)
                .id(student.getId())
                .build();
        client.delete(deleteReq);

        var getRes = getDocument(student.getId());
        assertFalse(getRes.found());
    }

    @Test
    public void testBulkCreateDocuments() throws IOException {
        var students = SampleData.get();
        var bulkRes = createDocuments(students);

        var expectedIds = students.stream()
                .map(Student::getId)
                .collect(Collectors.toList());
        var actualIds = bulkRes.items().stream()
                .map(BulkResponseItem::id)
                .collect(Collectors.toList());
        assertTrue(expectedIds.containsAll(actualIds));
        assertTrue(actualIds.containsAll(expectedIds));
    }

    private CreateResponse createDocument(Student student) throws IOException {
        var createReq = new CreateRequest.Builder<Student>()
                .index(INDEX_STUDENT)
                .id(student.getId())
                .document(student)
                .build();

        var res = client.create(createReq);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return res;
    }

    private BulkResponse createDocuments(List<Student> students) throws IOException {
        var requestBuilder = new BulkRequest.Builder().index(INDEX_STUDENT);
        for (var student : students) {
            var createOp = new CreateOperation.Builder<Student>()
                    .id(student.getId())
                    .document(student)
                    .build();
            var bulkOp = new BulkOperation.Builder()
                    .create(createOp)
                    .build();
            requestBuilder.operations(bulkOp);
        }

        var bulkReq = requestBuilder.build();
        var res = client.bulk(bulkReq);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return res;
    }

    private GetResponse<Student> getDocument(String id) throws IOException {
        var getReq = new GetRequest.Builder()
                .index(INDEX_STUDENT)
                .id(id)
                .build();

        return client.get(getReq, Student.class);
    }

}
