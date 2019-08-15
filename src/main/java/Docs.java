import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

public class Docs {
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
        deleteDoc(client, "test", "1"); // PASS
        indexDoc(client, "test", "1"); // PASS
        existDoc(client, "test", "1"); // PASS
        getDoc(client, "test", "1"); // PASS


        // updateDoc(client, "test",  "znbTSjn_QL2fYAM7Z8Yi4A"); // FAILED

        client.close();
    }


    /*
    * Index Doc
    * */
    public static void indexDoc(RestHighLevelClient client, String idx, String doc_id) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "Shane");
            builder.timeField("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        IndexRequest request = new IndexRequest(idx)
                .id(doc_id).source(builder);

        // Optional
        request.routing("routing");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        //request.version(2);
        //request.versionType(VersionType.EXTERNAL);
        //request.opType(DocWriteRequest.OpType.CREATE);
        //request.opType("create");
        //request.setPipeline("pipeline");

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        String index = indexResponse.getIndex();
        String id = indexResponse.getId();
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("Doc created with " + "index: " + index + ", id: " + id);
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("Doc updated with " + "index: " + index + ", id: " + id);
        }

        // Handle shards failure
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("Not all shards updated successfully");
            System.out.println("Total shards: " + shardInfo.getTotal() +
                    " Success shards: " + shardInfo.getSuccessful() +
                    " Failed shard: " + shardInfo.getFailed());
            //System.exit(-1);
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason(); // handle potential failures
                System.out.println(reason);
            }
        }

        // Handle doc conflict
        IndexRequest request1 = new IndexRequest(idx)
                .id(doc_id)
                .source("field", "value")
                .opType(DocWriteRequest.OpType.CREATE);
        try {
            IndexResponse response = client.index(request1, RequestOptions.DEFAULT);
        } catch(ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                System.out.println("Doc Conflict");
                System.out.println(e);
                System.exit(-1);
            }
        }

        // Handle version conflict
        IndexRequest request2 = new IndexRequest(idx)
                .id(doc_id)
                .source("field", "value");
                //.setIfSeqNo(10L)
                //.setIfPrimaryTerm(20);
        try {
            IndexResponse response = client.index(request2, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                System.out.println("Version Conflict");
                System.out.println(e);
                System.exit(-1);
            }
        }
    }


    /*
     * Get Doc
     */
    public static void getDoc(RestHighLevelClient client, String idx, String doc_id) throws IOException {
        GetRequest request = new GetRequest(idx, doc_id);

        // Optional
        //request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);

        /* Configure source inclusion and exclusion for specific fields
        String[] includes = new String[]{"message", "*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);
         */
        request.routing("routing");
        request.preference("preference");
        //request.realtime(false);
        request.refresh(true);
        //request.version(2);
        //request.versionType(VersionType.EXTERNAL);

        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);

        String index = getResponse.getIndex();
        String id = getResponse.getId();
        long version = getResponse.getVersion();
        if (getResponse.isExists()) {
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
            System.out.println("Response: " + sourceAsString); //+ ",\n" +
                    //sourceAsMap + ",\n" + sourceAsBytes);
        } else {
            System.out.println("Doc not found\n" + "index: " + index + ", id: " + id);
            System.exit(-1);
        }

        // Handle op on index not exist

        try {
            GetResponse getResponse1 = client.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println("Index not exist" + e);
                System.exit(-1);
            }
        }

        // Handle version conflict
        try {
            GetRequest request2 = new GetRequest(idx, doc_id);//.version(1);
            GetResponse getResponse2 = client.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT)  {
                System.out.println("Doc version conflict: " + e);
                System.exit(-1);
            }
        }
    }


    /*
    * Exist Doc
    * */
    public static void existDoc(RestHighLevelClient client, String idx, String doc_id) throws IOException {
        GetRequest request = new GetRequest(idx, doc_id);

        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");

        // Optional

        /* Configure source inclusion and exclusion for specific fields
        String[] includes = new String[]{"message", "*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);
         */
        request.routing("routing");
        request.preference("preference");
        //request.realtime(false);
        request.refresh(true);
        //request.version(2);
        //request.versionType(VersionType.EXTERNAL);

        boolean exists = client.exists(request, RequestOptions.DEFAULT);

        if (exists) {
            System.out.println("Doc " + doc_id + " exists");
        } else {
            System.out.println("Doc " + doc_id + " not exists");
        }
    }


    /*
    * Delete Doc
    * */
    public static void deleteDoc(RestHighLevelClient client, String idx, String doc_id) throws IOException {
        DeleteRequest request = new DeleteRequest(idx, doc_id);

        // Optional
        request.routing("routing");
        request.timeout(TimeValue.timeValueMinutes(2));
        //request.timeout("2m");
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        //request.setRefreshPolicy("wait_for");
        //request.version(2);
        //request.versionType(VersionType.EXTERNAL);

        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);

        // the document to be deleted was not found
        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            System.out.println("Doc " + doc_id + " not found");
            System.exit(-1);
        }

        String index = deleteResponse.getIndex();
        String id = deleteResponse.getId();
        long version = deleteResponse.getVersion();
        System.out.println("Response:\nindex: " + index + " id: " +
                id + "  version: " + version);

        ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("Not all shards updated successfully");
            System.out.println("Total shards: " + shardInfo.getTotal() +
                    " Success shards: " + shardInfo.getSuccessful() +
                    " Failed shard: " + shardInfo.getFailed());
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason(); // handle potential failures
                System.out.println(reason);
            }
        }

        try {
            DeleteResponse deleteResponse2 = client.delete(
                    new DeleteRequest(idx, doc_id),//.setIfSeqNo(100).setIfPrimaryTerm(2),
                    RequestOptions.DEFAULT);
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.CONFLICT) {
                System.out.println("Version Conflict");
                System.out.println(exception);
                System.exit(-1);
            }
        }

        DeleteResponse deleteResponse1 = client.delete(request, RequestOptions.DEFAULT);

        // check deletion result
        if (deleteResponse1.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            System.out.println("Doc " + doc_id + " has been deleted");
        }
    }
    
    /*
     * Update API allows to update an existing document by using
     * a script or by passing a partial document.
     */
    public static void updateDoc(RestHighLevelClient client, String idx, String doc_id) throws IOException {
        UpdateRequest request = new UpdateRequest(idx, doc_id);

        Map<String, Object> parameters = Collections.singletonMap("count", 4);

        Script inline = new Script(ScriptType.INLINE, "painless",
                "ctx._source.field += params.count", parameters);
        request.script(inline);

        /* Or as stored script
         *
        Script stored = new Script(
                ScriptType.STORED, null, "increment-field", parameters);
        request.script(stored);
         */

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.timeField("updated", new Date());
            builder.field("reason", "daily update");
            builder.field("created by", "Shane"); // new contents
        }
        builder.endObject();
        UpdateRequest request1 = new UpdateRequest(idx, doc_id)
                .doc(builder);

        // Optional
        request.routing("routing");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        request.retryOnConflict(3);
        request.fetchSource(true);
        String[] includes = new String[]{"updated", "r*"};
        String[] excludes = Strings.EMPTY_ARRAY;
        request.fetchSource(
                new FetchSourceContext(true, includes, excludes));
        request.detectNoop(true);
        request.scriptedUpsert(true);
        //request.docAsUpsert(true);
        request.waitForActiveShards(2);
        request.waitForActiveShards(ActiveShardCount.ALL);

        /* The asynchronous method

        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                System.out.println("Update info get");
                // TODO
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("Response failed");
                System.exit(-1);
            }
        };
        client.updateAsync(request, RequestOptions.DEFAULT, listener);

         */

        UpdateResponse updateResponse = client.update(
                request, RequestOptions.DEFAULT
        );

        String index = updateResponse.getIndex();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("Doc Created " + "index: " + index + ", id: " + id);
            System.out.println(version);
        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("Doc Updated " + "index: " + index + ", id: " + id);
            System.out.println(version);
        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
            System.out.println("Doc Deleted " + "index: " + index + ", id: " + id);
            System.out.println(version);
        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
            System.out.println("No operation on Doc " + "index: " + index + ", id: " + id);
            System.out.println(version);
        } else {
            System.out.println("Unresolved operation");
            System.exit(-1);
        }

        // source retrieval should be enabled in the UpdateRequest through the fetchSource method
        GetResult result = updateResponse.getGetResult();
        if (result.isExists()) {
            String sourceAsString = result.sourceAsString();
            Map<String, Object> sourceAsMap = result.sourceAsMap();
            byte[] sourceAsBytes = result.source();
            System.out.println("Doc source(String type): " + sourceAsString);
        } else {
            System.out.println("No source of doc in response");
            System.exit(-1);
        }


        // Check shard failures
        ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("Not all shards updated successfully");
            System.exit(-1);
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason(); // handle potential failures
            }
        }

        // Handle update request to a not exist doc
        UpdateRequest request2 = new UpdateRequest(idx, "does_not_exist")
                .doc("field", "value");
        try {
            UpdateResponse updateResponse1 = client.update(
                    request2, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                System.out.println("Doc not exist");
                System.out.println(e);
                System.exit(-1);
            }
        }

        // Version conflict
        UpdateRequest request3 = new UpdateRequest(idx, doc_id)
                .doc("field", "value")
                .setIfSeqNo(101L)
                .setIfPrimaryTerm(200L);
        try {
            UpdateResponse updateResponse1 = client.update(
                    request3, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                System.out.println("Version Conflict");
                System.out.println(e);
                System.exit(-1);
            }
        }
    }

}
