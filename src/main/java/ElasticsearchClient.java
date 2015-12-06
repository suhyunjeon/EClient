import com.google.gson.Gson;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Jest client 를 이용하여 elasticsearch 에 데이터를 저장한다.
 * Mockup 코드
 *
 * User: suhyunjeon
 * Date: 2015. 12. 6.
 */
public class ElasticsearchClient {
    
    final static Logger logger = LoggerFactory.getLogger(ElasticsearchClient.class);

    public static void main(String args[]) throws IOException {
        JestClient client = jestClient();

        createIndex(client);
        indexDocument(client);
        getDocument(client);
//        search(client);
    }

    private static void getDocument(JestClient client) throws IOException {
        Get get = new Get.Builder("diet", "1").type("dieter").build();
        JestResult result = client.execute(get);

        logger.debug(result.getJsonObject().toString());
        logger.debug(result.getValue("_source").toString());
    }

    private static void search(JestClient client) throws IOException {
        Search search = new Search.Builder("").addIndex("diet").addType("dieter").build();
        SearchResult result = client.execute(search);
        logger.debug(result.getTotal().toString());
    }

    private static void createIndex(JestClient client) throws IOException {
        CreateIndex createIndex = new CreateIndex.Builder("diet").build();
        client.execute(createIndex);

        logger.debug(createIndex.getURI());
        logger.debug(createIndex.getRestMethodName());
        
        logger.debug(new Gson().toJson(createIndex.getData(new Gson())));
    }

    private static void indexDocument(JestClient client) throws IOException {
        String source = "{\"user\":\"tasha\", \"plan\":\"no food\"}";
        Index index = new Index.Builder(source).index("diet").type("dieter").id("1").build();
        client.execute(index);

        logger.debug(index.getRestMethodName());
        logger.debug(index.getURI());
    }

    private static JestClient jestClient() {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        return factory.getObject();
    }
}
