package samples;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.mapping.IntegerNumberProperty;
import org.opensearch.client.opensearch._types.mapping.Property;
import org.opensearch.client.opensearch._types.mapping.TypeMapping;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;
import samples.util.IndexData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BulkUpdateTest {
    private static final Logger LOGGER = LogManager.getLogger(BulkUpdateTest.class);

    public static void main(String[] args) {
        try {
            var client = SampleClient.createRestClient();

            final var indexName = "my-index";

            if (!client.indices().exists(r -> r.index(indexName)).value()) {
                LOGGER.info("Creating index {}", indexName);
                IndexSettings settings = new IndexSettings.Builder().numberOfShards("2").numberOfReplicas("1").build();
                TypeMapping mapping = new TypeMapping.Builder().properties(
                        "age",
                        new Property.Builder().integer(new IntegerNumberProperty.Builder().build()).build()
                ).build();
                CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder().index(indexName)
                        .settings(settings)
                        .mappings(mapping)
                        .build();
                client.indices().create(createIndexRequest);
            }
            LOGGER.info("Indexing documents");
            IndexData indexData = new IndexData("Document 1", "Text for document 1");
            IndexRequest<IndexData> indexRequest = new IndexRequest.Builder<IndexData>().index(indexName)
                    .id("1")
                    .document(indexData)
                    .build();
            client.index(indexRequest);
            indexData = new IndexData("Document 2", "Text for document 2");
            indexRequest = new IndexRequest.Builder<IndexData>().index(indexName).id("2").document(indexData).build();
            client.index(indexRequest);

            // wait for the document to index
            Thread.sleep(3000);

            IndexData searchedData;
            ArrayList<BulkOperation> ops = new ArrayList<>();

            for (var hit : getSearchResults(client, "text", "Text for document")) {
                LOGGER.info("Found {} with score {} and id {}", hit.source(), hit.score(), hit.id());
                searchedData = hit.source();
                IndexData finalSearchedData = searchedData;
                finalSearchedData.setText("Updated document");
                ops.add(new BulkOperation.Builder().index(IndexOperation.of(io -> io.index(indexName).id(hit.id()).document(finalSearchedData))).build());
                BulkRequest request = new BulkRequest.Builder().operations(o -> o.update(u -> u.index(indexName)
                        .id(hit.id()).document(finalSearchedData))).build();
                BulkResponse bulkResponse = client.bulk(request);
                LOGGER.info("Bulk response items: {}", bulkResponse.items().size());
            }
            BulkRequest.Builder bulkReq = new BulkRequest.Builder().index(indexName).operations(ops).refresh(Refresh.WaitFor);
            BulkResponse bulkResponse = client.bulk(bulkReq.build());
            LOGGER.info("Bulk response items: {}", bulkResponse.items().size());

            // wait for the changes to reflect
            Thread.sleep(3000);
            for (var hit : getSearchResults(client, "text", "Text for document")) {
                LOGGER.info("Updated record: id = {} | Title = {} | Text = {}", hit.id(), hit.source().getTitle(), hit.source().getText());
            }

            LOGGER.info("Deleting index {}", indexName);
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder().index(indexName).build();
            client.indices().delete(deleteIndexRequest);

        } catch (Exception e) {
            LOGGER.error("Unexpected exception", e);
        }
        LOGGER.info("Completed");

    }

    private static List<Hit<IndexData>> getSearchResults(OpenSearchClient client, String field, String value) throws IOException {
        SearchRequest searchRequest = new SearchRequest.Builder().query(
                q -> q.match(m -> m.field(field).query(FieldValue.of(value)))
        ).build();

        SearchResponse<IndexData> searchResponse = client.search(searchRequest, IndexData.class);
        return searchResponse.hits().hits();
    }
}
