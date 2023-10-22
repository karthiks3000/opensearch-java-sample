package samples;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import samples.util.MyDoc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

public class BulkUpdateTest {
    private static final Logger LOGGER = LogManager.getLogger(BulkUpdateTest.class);

    public static void main(String[] args) {
        try {
            Issue590Test();
        } catch (Exception e) {
            LOGGER.error(e.toString());
        }
    }

    private static void Issue590Test() throws IOException, InterruptedException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        var client = SampleClient.create();
        final var indexName = "my-index";
        if (!client.indices().exists(r -> r.index(indexName)).value()) {
            LOGGER.info("Creating index {}", indexName);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder().index(indexName)
                    .build();
            client.indices().create(createIndexRequest);
        }

        MyDoc doc = new MyDoc();
        doc.setId(1);
        doc.setStatus("active");

        IndexRequest<MyDoc> indexRequest = new IndexRequest.Builder<MyDoc>().index(indexName)
                .id("1")
                .document(doc)
                .build();
        client.index(indexRequest);
        // wait for the changes to reflect
        Thread.sleep(3000);

        doc.setStatus("inactive");
        BulkRequest request = new BulkRequest.Builder().operations(o -> o.update(u -> u.index(indexName).id(String.valueOf(1)).document(doc))).build();
        BulkResponse bulkResponse = client.bulk(request);
        LOGGER.info("Bulk response items: {}", bulkResponse.items().size());

        Thread.sleep(3000);
        SearchResponse<MyDoc> searchResponse = client.search(s -> s.index(indexName), MyDoc.class);
        for (var hit : searchResponse.hits().hits()) {
            LOGGER.info("Updated record: id = {} | Status = {}", hit.source().getId(), hit.source().getStatus());
        }
        LOGGER.info("Deleting index {}", indexName);
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest.Builder().index(indexName).build();
        client.indices().delete(deleteIndexRequest);
    }

}
