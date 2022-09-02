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
import java.util.*;
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

    @Test
    public void testFieldValueFactorScore() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var fieldValueFactorFunc = new FieldValueFactorScoreFunction.Builder()
                .field("grade")
                .factor(0.5)
                .modifier(FieldValueFactorModifier.Square)
                .missing(0.0)
                .build()
                ._toFunctionScore();

        var expectedScore = Map.of(
                "101", 4.0,
                "102", 2.25,
                "103", 1.0,
                "104", 0.25
        );

        var searchRes = search(List.of(fieldValueFactorFunc));
        assertDocumentScore(searchRes, expectedScore);
    }

    @Test
    public void testFilterAndWeightScore() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var departmentQuery = TermQuery.of(b ->
                        b.field("department.keyword").value("財務金融"))
                ._toQuery();
        var departmentFunc = new FunctionScore.Builder()
                .filter(departmentQuery)
                .weight(3.0)
                .build();

        var courseQuery = TermQuery.of(b ->
                        b.field("courses.name.keyword").value("程式設計"))
                ._toQuery();
        var courseFunc = new FunctionScore.Builder()
                .filter(courseQuery)
                .weight(1.5)
                .build();

        var gradeFactor = new FieldValueFactorScoreFunction.Builder()
                .field("grade")
                .build();
        var gradeFunc = new FunctionScore.Builder()
                .fieldValueFactor(gradeFactor)
                .weight(0.5)
                .build();

        var expectedScore = Map.of(
                "103", 5.5,
                "101", 5.0,
                "102", 1.5,
                "104", 0.5
        );

        var searchRes = search(List.of(departmentFunc, courseFunc, gradeFunc));
        assertDocumentScore(searchRes, expectedScore);
    }

    @Test
    public void testGaussFunctionScore_Number() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var placement = new DecayPlacement.Builder()
                .origin(JsonData.of(100))
                .offset(JsonData.of(15))
                .scale(JsonData.of(10))
                .decay(0.5)
                .build();
        var decayFunc = new DecayFunction.Builder()
                .field("conductScore")
                .placement(placement)
                .build();
        var gaussFunc = new FunctionScore.Builder()
                .gauss(decayFunc)
                .build();

        var expectedScore = Map.of(
                "103", 1.0,
                "102", 0.9726,
                "101", 0.4322,
                "104", 0.2570
        );

        var searchRes = search(List.of(gaussFunc));
        assertDocumentScore(searchRes, expectedScore);
    }

    @Test
    public void testGaussFunctionScore_Date() throws IOException {
        var students = SampleData.get();
        createDocuments(students);

        var now = new Date(1658592000000L);

        var placement = new DecayPlacement.Builder()
                .origin(JsonData.of(now.getTime()))
                .offset(JsonData.of("90d"))
                .scale(JsonData.of("270d"))
                .decay(0.5)
                .build();
        var decayFunc = new DecayFunction.Builder()
                .field("englishIssuedDate")
                .placement(placement)
                .build();
        var gaussFunc = new FunctionScore.Builder()
                .gauss(decayFunc)
                .build();

        var expectedScore = Map.of(
                "102", 1.0,
                "104", 0.8195,
                "101", 0.2378,
                "103", 0.1132
        );

        var searchRes = search(List.of(gaussFunc));
        assertDocumentScore(searchRes, expectedScore);
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

    private SearchResponse<Student> search(List<FunctionScore> functions) throws IOException {
        var matchAllQuery = MatchAllQuery.of(b -> b)._toQuery();

        var query = new FunctionScoreQuery.Builder()
                .query(matchAllQuery)
                .functions(functions)
                .scoreMode(FunctionScoreMode.Sum)
                .boostMode(FunctionBoostMode.Replace)
                .maxBoost(100.0)
                .build()
                ._toQuery();

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

    private void assertDocumentScore(SearchResponse<Student> res, Map<String, Double> expectedScoreMap) {
        var actualScoreMap = new HashMap<String, Double>();
        res.hits().hits().forEach(hit -> actualScoreMap.put(hit.id(), hit.score()));

        expectedScoreMap.forEach((docId, score) ->
                assertEquals(actualScoreMap.get(docId), score, 0.0001));
    }

}
