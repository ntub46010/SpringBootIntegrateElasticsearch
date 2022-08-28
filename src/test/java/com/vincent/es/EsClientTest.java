package com.vincent.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.json.JsonData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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


    @Test
    public void testTermFieldWithSingleValue() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var query = new TermQuery.Builder()
                .field("grade")
                .value(3)
                .build()
                ._toQuery();
        var searchRes = search(query);

        // Mario
        assertDocumentIds(true, searchRes, "102");
    }

    @Test
    public void testTermsFieldWithMultipleValue() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var fieldValues = Stream.of("資訊管理", "企業管理")
                .map(value -> FieldValue.of(b -> b.stringValue(value)))
                .collect(Collectors.toList());
        var termsQueryField = new TermsQueryField.Builder()
                .value(fieldValues)
                .build();

        var query = new TermsQuery.Builder()
                .field("departments.keyword")
                .terms(termsQueryField)
                .build()
                ._toQuery();
        var searchRes = search(query);

        // Vincent, Winnie
        assertDocumentIds(true, searchRes, "103", "104");
    }

    @Test
    public void testNumberRangeQuery() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var query = new RangeQuery.Builder()
                .field("grade")
                .gte(JsonData.of(2))
                .lte(JsonData.of(4))
                .build()
                ._toQuery();
        var searchRes = search(query);

        // Vincent, Dora, Mario
        assertDocumentIds(true, searchRes, "103", "101", "102");
    }

    @Test
    public void testDateRangeQuery() throws IOException, ParseException {
        var students = SampleData.get();
        createDocuments(students);

        var sdf = new SimpleDateFormat("yyyy-MM-dd");
        var startDate = sdf.parse("2021-07-01");
        var endDate = sdf.parse("2022-07-01");

        var query = new RangeQuery.Builder()
                .field("englishIssuedDate")
                .gte(JsonData.of(startDate))
                .lt(JsonData.of(endDate))
                .build()
                ._toQuery();
        var searchRes = search(query);

        // Winnie, Mario
        assertDocumentIds(true, searchRes, "104", "102");
    }

    @Test
    public void testMatchSingleField() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var query = new MatchQuery.Builder()
                .field("introduction")
                .query("company career")
                .build()
                ._toQuery();
        var searchRes = search(query);

        // Vincent, Winnie
        assertDocumentIds(true, searchRes, "103", "104");
    }

    @Test
    public void testComplexBoolQuery() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var gradeQuery = RangeQuery.of(b ->
                        b.field("grade").lt(JsonData.of(4)))
                ._toQuery();

        var jobPrimaryQuery = TermQuery.of(b ->
                        b.field("job.primary").value(false))
                ._toQuery();

        var courseQuery = TermQuery.of(b ->
                        b.field("courses.name.keyword").value("會計學"))
                ._toQuery();

        var departmentQuery = TermQuery.of(b ->
                        b.field("departments.keyword").value("財務金融"))
                ._toQuery();

        var query = new BoolQuery.Builder()
                .must(gradeQuery)
                .mustNot(jobPrimaryQuery)
                .should(courseQuery, departmentQuery)
                .build()
                ._toQuery();
        var searchRes = search(query);

        // Vincent
        assertDocumentIds(true, searchRes, "103");
    }

    @Test
    public void testSortWithMultipleFields() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var matchAll = new MatchAllQuery.Builder().build();
        var query = Query.of(b -> b.matchAll(matchAll));

        var coursePointFieldSort = new FieldSort.Builder()
                .field("courses.point")
                .mode(SortMode.Max)
                .order(SortOrder.Desc)
                .build();
        var coursePointSortOp = SortOptions.of(b -> b.field(coursePointFieldSort));

        var nameFieldSort = new FieldSort.Builder()
                .field("name.keyword")
                .order(SortOrder.Asc)
                .build();
        var nameSortOp = SortOptions.of(b -> b.field(nameFieldSort));

        var searchReq = new SearchRequest.Builder()
                .index(INDEX_STUDENT)
                .query(query)
                .sort(coursePointSortOp, nameSortOp)
                .build();
        var searchRes = client.search(searchReq, Student.class);

        // Mario -> Vincent -> Dora -> Winnie
        assertDocumentIds(false, searchRes, "102", "103", "101", "104");
    }

    @Test
    public void testPaging() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var matchAll = new MatchAllQuery.Builder().build();
        var query = Query.of(b -> b.matchAll(matchAll));

        var fieldSort = new FieldSort.Builder()
                .field("conductScore")
                .order(SortOrder.Desc)
                .build();
        var sortOp = SortOptions.of(b -> b.field(fieldSort));

        var searchReq = new SearchRequest.Builder()
                .index(INDEX_STUDENT)
                .query(query)
                .sort(sortOp)
                .from(0)
                .size(2)
                .build();
        var searchRes = client.search(searchReq, Student.class);

        // Vincent -> Mario
        assertDocumentIds(false, searchRes, "103", "102");
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

    private SearchResponse<Student> search(Query query) throws IOException {
        var searchReq = new SearchRequest.Builder()
                .index(INDEX_STUDENT)
                .query(query)
                .build();
        return client.search(searchReq, Student.class);
    }

    private void assertDocumentIds(boolean ignoreOrder, SearchResponse<Student> res, String... expectedIdArray) {
        var actualIds = res
                .hits()
                .hits()
                .stream()
                .map(Hit::source)
                .map(Student::getId)
                .collect(Collectors.toList());
        var expectedIds = List.of(expectedIdArray);

        if (ignoreOrder) {
            assertTrue(expectedIds.containsAll(actualIds));
            assertTrue(actualIds.containsAll(expectedIds));
        } else {
            assertEquals(expectedIds, actualIds);
        }
    }

}
