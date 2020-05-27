package com.company;

import com.google.common.io.CharStreams;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

/**
 * This example uses the Java Low Level Client api.
 */
@Slf4j
public class ArticleMainLL {

  public static void main(String[] args) {
    try {
      RestClient restClient = RestClient.builder(
        new HttpHost("localhost", 9200, "http"),
        new HttpHost("localhost", 9201, "http")).build();

      // create index
      UUID uuid = UUID.randomUUID();
      String indexName = "articles-" + uuid.toString();

      Request ciRequest = new Request("PUT", "/" + indexName);
      Response ciResponse = restClient.performRequest(ciRequest);
      log.info(ciResponse.toString());

      // populate index
      List<String> lines = Files.readAllLines(Paths.get("article-data.json"), StandardCharsets.UTF_8);

      StringBuilder sb = new StringBuilder(1024);
      for (String line : lines) {
        sb.append(line);
      }
      JSONParser parser = new JSONParser();
      JSONArray articlesArray = (JSONArray) parser.parse(sb.toString());

      for (Object object : articlesArray) {
        JSONObject article = (JSONObject) object;

        /**
         * You can index a new JSON document with the _doc or _create resource. Using _create guarantees that
         * the document is only indexed if it does not already exist.
         * To update an existing document, you must use the _doc resource.
         */
        String type = "_doc";

        Request indexRequest = new Request("POST", "/" + indexName + "/" + type + "?refresh=wait_for");
        indexRequest.setJsonEntity(((JSONObject) article).toJSONString());

        Response indexResponse = restClient.performRequest(indexRequest);
        log.info(indexResponse.toString());
      }

      // run a search
      JSONObject jsonObject1 = new JSONObject();
      JSONObject jsonObject2 = new JSONObject();
      jsonObject2.put("match_all", jsonObject1);
      JSONObject jsonObject3 = new JSONObject();
      jsonObject3.put("query", jsonObject2);

      Request searchRequest1 = new Request("GET", "/" + indexName + "/_search");
      searchRequest1.setJsonEntity(jsonObject3.toJSONString());

      Response searchResponse1 = restClient.performRequest(searchRequest1);
      log.info(searchResponse1.toString());
      InputStream searchResponseStream1 = searchResponse1.getEntity().getContent();

      String text1 = null;
      try (final Reader reader = new InputStreamReader(searchResponseStream1)) {
        text1 = CharStreams.toString(reader);
      }

      JSONObject responseEntityJSON1 = (JSONObject) parser.parse(text1);
      JSONObject outerHitList1 = (JSONObject) responseEntityJSON1.get("hits");
      JSONArray innerHitList1 = (JSONArray) outerHitList1.get("hits");

      for (Object object : innerHitList1) {
        JSONObject hit = (JSONObject) object;
        String hitId = (String) hit.get("_id");
        Double hitScore = (Double) hit.get("_score");
        JSONObject hitSource = (JSONObject) hit.get("_source");

        log.info("Id:" + hitId + ", Score:" + hitScore);
        log.info(hitSource.toJSONString());
      }

      // run another search
      JSONObject jsonObject4 = new JSONObject();
      jsonObject4.put("description", "password");
      JSONObject jsonObject5 = new JSONObject();
      jsonObject5.put("match", jsonObject4);
      JSONObject jsonObject6 = new JSONObject();
      jsonObject6.put("query", jsonObject5);

      Request searchRequest2 = new Request("GET", "/" + indexName + "/_search");
      searchRequest2.setJsonEntity(jsonObject6.toJSONString());

      Response searchResponse2 = restClient.performRequest(searchRequest2);
      log.info(searchResponse2.toString());
      InputStream searchResponseStream2 = searchResponse2.getEntity().getContent();

      String text2 = null;
      try (final Reader reader = new InputStreamReader(searchResponseStream2)) {
        text2 = CharStreams.toString(reader);
      }

      JSONObject responseEntityJSON2 = (JSONObject) parser.parse(text2);
      JSONObject outerHitList2 = (JSONObject) responseEntityJSON2.get("hits");
      JSONArray innerHitList2 = (JSONArray) outerHitList2.get("hits");

      for (Object object : innerHitList2) {
        JSONObject hit = (JSONObject) object;
        String hitId = (String) hit.get("_id");
        Double hitScore = (Double) hit.get("_score");
        JSONObject hitSource = (JSONObject) hit.get("_source");

        log.info("Id:" + hitId + ", Score:" + hitScore);
        log.info(hitSource.toJSONString());
      }

      // delete index
      Request diRequest = new Request("DELETE", "/" + indexName);
      Response diResponse = restClient.performRequest(diRequest);
      log.info(diResponse.toString());

      restClient.close();
      log.info("Done");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
