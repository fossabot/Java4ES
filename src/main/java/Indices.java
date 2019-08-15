import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class Indices {
    public static void main(String[] args) throws IOException
    {
        /*
        * Typically initialize a client for ES version older than 7.0
        Settings settings = Settings.builder()
                .put("cluster.name", "prob").build();
        TransportClient transportClient = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9301));
        * */

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                        // new HttpHost("localhost", 9201, "http")
                )
        );

        // test units
        closeIndex(client, "test"); // PASS
        deleteIndex(client); // PASS
        addIndexByBuilder(client); // PASS
        getIndex(client); // PASS
        openIndex(client, "test"); //PASS

        client.close();
    }

    public static void addIndexByBuilder(RestHighLevelClient client) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest("test");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );


        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startObject("properties");
            {
                builder.startObject("message");
                {
                    builder.field("type", "text");
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.mapping(builder);

        /*
        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        request.mapping(mapping);
        */

        // Optional
        request.setTimeout(TimeValue.timeValueMinutes(2));
        request.setMasterTimeout(TimeValue.timeValueMinutes(1));
        request.waitForActiveShards(ActiveShardCount.DEFAULT);

        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        boolean ack = createIndexResponse.isAcknowledged();
        boolean shardAck = createIndexResponse.isShardsAcknowledged();
        System.out.println("Create index: " + ack);
        System.out.println("Create shard index: " + shardAck);
    }

    /*
    * Add index by Elasticsearch providing built-in helpers to generate JSON content.
    *
    public static void addIndexByBuilder() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
                .field("user", "Shane")
                .field("postDate", new Date())
                .field("message", "trying out ES")
            .endObject();
        IndexResponse response = client.prepareIndex("forTest", "_doc", "1")
                .setSource(builder).get();
        System.out.println(response.getVersion());
    }
    */


    /* Delete index by name */
    public static void deleteIndex(RestHighLevelClient client) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("test");

        // Optional
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        request.masterNodeTimeout("1m");
        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        boolean ack = deleteIndexResponse.isAcknowledged();
        System.out.println("Delete index: " + ack);
    }


    /* Get Index by name */
    public static void getIndex(RestHighLevelClient client) throws IOException {
        GetIndexRequest request = new GetIndexRequest("test");

        // Optional
        request.local(false);
        request.humanReadable(true);
        request.includeDefaults(false);
        request.indicesOptions();

        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println("Get index: " + exists);
    }


    /* Open Index by name */
    public static void openIndex(RestHighLevelClient client, String indices) throws IOException {
        OpenIndexRequest request = new OpenIndexRequest(indices);

        // Optional
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        request.masterNodeTimeout("1m");
        request.waitForActiveShards(2);
        request.waitForActiveShards(ActiveShardCount.DEFAULT);
        request.indicesOptions(IndicesOptions.strictExpandOpen());

        OpenIndexResponse openIndexResponse = client.indices().open(request, RequestOptions.DEFAULT);

        boolean ack = openIndexResponse.isAcknowledged();
        boolean shardsAck = openIndexResponse.isShardsAcknowledged();
        System.out.println("Open index " + indices + ": " + ack);
        System.out.println("Open shard index " + indices + ": " + shardsAck);
    }


    /* Close Index by name */
    public static void closeIndex(RestHighLevelClient client, String indices) throws IOException {
        CloseIndexRequest request = new CloseIndexRequest(indices);

        // Optional
        request.timeout(TimeValue.timeValueMinutes(2));
        request.timeout("2m");
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        request.masterNodeTimeout("1m");
        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        AcknowledgedResponse closeIndexResponse = client.indices().close(request, RequestOptions.DEFAULT);
        boolean ack = closeIndexResponse.isAcknowledged();
        System.out.println("Close index " + indices + ": " + ack);
    }

}
