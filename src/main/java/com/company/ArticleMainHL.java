package com.company;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

/**
 * This example uses the Java High Level Client api.
 */
@Slf4j
public class ArticleMainHL {

  public static void main(String[] args) {
    try {
      RestHighLevelClient client = new RestHighLevelClient(
        RestClient.builder(
          new HttpHost("localhost", 9200, "http"),
          new HttpHost("localhost", 9201, "http")));

      // create index
      UUID uuid = UUID.randomUUID();
      String indexName = "articles-" + uuid.toString();

      CreateIndexRequest ciRequest = new CreateIndexRequest(indexName);
      ciRequest.settings(Settings.builder()
        .put("index.number_of_shards", 3)
        .put("index.number_of_replicas", 2)
      );
      CreateIndexResponse ciResponse = client.indices().create(ciRequest, RequestOptions.DEFAULT);
      log.info(ciResponse.toString());

      // populate index
      List<String> lines = Files.readAllLines(Paths.get("article-data.json"), StandardCharsets.UTF_8);

      StringBuilder sb = new StringBuilder(1024);
      for (String line : lines) {
        sb.append(line);
      }
      JSONParser parser = new JSONParser();
      JSONArray articlesArray = (JSONArray) parser.parse(sb.toString());

      for (Object article : articlesArray) {
        IndexRequest request = new IndexRequest(indexName);

        request.source(((JSONObject) article).toJSONString(), XContentType.JSON);

        /**
         * You can index a new JSON document with the _doc or _create resource. Using _create guarantees that
         * the document is only indexed if it does not already exist.
         * To update an existing document, you must use the _doc resource.
         */
        request.type("_doc");

        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        log.info(indexResponse.status().toString());
      }

      // run a search
      SearchRequest searchRequest1 = new SearchRequest(indexName);
      SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
      searchSourceBuilder1.query(QueryBuilders.matchAllQuery());
      searchRequest1.source(searchSourceBuilder1);

      SearchResponse searchResponse = client.search(searchRequest1, RequestOptions.DEFAULT);
      log.info(searchResponse.toString());

      for (SearchHit hit : searchResponse.getHits().getHits()) {
        log.info("Id:" + hit.getId() + ", Score:" + hit.getScore());
        log.info(hit.getSourceAsString());
      }

      // run another search
      SearchRequest searchRequest2 = new SearchRequest(indexName);
      SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder();
      String fieldName = "description";
      String text = "password";
      searchSourceBuilder2.query(QueryBuilders.matchQuery(fieldName, text));
      searchRequest2.source(searchSourceBuilder2);

      SearchResponse searchResponse2 = client.search(searchRequest2, RequestOptions.DEFAULT);
      log.info(searchResponse2.toString());

      for (SearchHit hit : searchResponse2.getHits().getHits()) {
        log.info("Id:" + hit.getId() + ", Score:" + hit.getScore());
        log.info(hit.getSourceAsString());
      }

      // delete index
      DeleteIndexRequest diRequest = new DeleteIndexRequest(indexName);
      AcknowledgedResponse diResponse = client.indices().delete(diRequest, RequestOptions.DEFAULT);
      log.info(diResponse.toString());

      client.close();
      log.info("Done");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
