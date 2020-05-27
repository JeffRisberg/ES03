package com.company;

import com.company.common.ISearch;
import com.company.common.SearchQuery;
import com.company.common.SearchQueryClause;
import com.company.es.ESSearchImpl;
import com.company.service.CountService;
import com.company.service.DataService;
import com.company.service.DeleteService;
import com.company.service.IngestService;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

@Slf4j
public class ProductMain {
  public static void main(String[] args) {
    String clusterName = "elasticsearch";
    String indexName = "annotated-products";

    List<String> indexes = new ArrayList<String>();
    indexes.add(indexName);

    Settings settings = Settings.builder().put("cluster.name", clusterName).build();

    Client client =
        new PreBuiltTransportClient(settings)
            .addTransportAddress(new TransportAddress(new InetSocketAddress("127.0.0.1", 9300)));

    CountService countService = new CountService(client, indexName);
    DataService dataService = new DataService(client, indexName);
    IngestService ingestService = new IngestService(client, indexName);
    DeleteService deleteService = new DeleteService(client, indexName);

    // Count
    System.out.println("getMatchAllQueryCount " + countService.getMatchAllQueryCount());
    System.out.println("getTermAndTermQueryData " + countService.getBoolQueryCount("nulla"));
    System.out.println("getPhraseQueryCount " + countService.getPhraseQueryCount("lobster"));

    // Data
    System.out.println("getMatchAllQueryData");
    dataService.getMatchAllQueryData().forEach(item -> System.out.println("a: " + item));

    System.out.println("getTermAndTermQueryData");
    dataService
        .getBoolQueryData("nulla", "lobster")
        .forEach(item -> System.out.println("b: " + item));

    System.out.println("getPhraseQueryData");
    dataService.getPhraseQueryData("lobster").forEach(item -> System.out.println("c: " + item));

    // Set up documents
    String json1 =
        "{"
            + "\"name\":\"Yellow-black Furby\","
            + "\"price\":\"120\","
            + "\"description\":\"A Furby is a really cute toy. This one has yellow-black coloring.  Its name is Noo-Loo\","
            + "\"in_stock\":\"0\""
            + "}";

    String json2 =
        "{"
            + "\"name\":\"Terran Marine\","
            + "\"price\":\"30\","
            + "\"description\":\"From the Starcraft game.  Made by Blizzard.\","
            + "\"in_stock\":\"2\""
            + "}";

    String json3 =
        "{"
            + "\"name\":\"Zerg Hydralisk\","
            + "\"price\":\"35\","
            + "\"description\":\"From the Starcraft game.  Made by Blizzard.\","
            + "\"in_stock\":\"3\""
            + "}";

    String json4 =
        "{"
            + "\"name\":\"Protoss Zealot\","
            + "\"price\":\"40\","
            + "\"description\":\"From the Starcraft game.  Made by Blizzard.\","
            + "\"in_stock\":\"0\""
            + "}";

    // Ingest single record
    System.out.println("\nIngestService response::: " + ingestService.ingest("default", json1));

    // Ingest batch of records
    System.out.println(
        "\nIngestService response::: "
            + ingestService.ingest("default", Arrays.asList(json2, json3, json4)));

    // Count records
    System.out.println("getMatchAllQueryCount " + countService.getMatchAllQueryCount());

    // Delete
    System.out.println("delete by query " + deleteService.deleteByQuery("furby").getDeleted());

    System.out.println("delete by query " + deleteService.deleteByQuery("blizzard").getDeleted());

    // Count records
    System.out.println("getMatchAllQueryCount " + countService.getMatchAllQueryCount());

    client.close();

    System.out.println("\n\nNEW IMPLEMENTATION STARTS HERE\n");

    ISearch search = new ESSearchImpl("127.0.0.1", 9300, "elasticsearch", indexes);
    SearchQueryClause clause1 =
        new SearchQueryClause(SearchQueryClause.ClauseType.MATCH_TERM, "name", "lobster");
    SearchQueryClause clause2 =
        new SearchQueryClause(SearchQueryClause.ClauseType.MATCH_TERM, "description", "nulla");
    SearchQueryClause clause3 =
        new SearchQueryClause(SearchQueryClause.ClauseType.MATCH_PHRASE, "name", "lobster");

    // Count
    System.out.println("getMatchAllQueryCount " + search.count(SearchQuery.builder().build()));
    System.out.println(
        "getBoolQueryCount "
            + search.count(SearchQuery.builder().clause(clause1).clause(clause2).build()));
    System.out.println(
        "getPhraseQueryCount " + search.count(SearchQuery.builder().clause(clause3).build()));

    // Data
    System.out.println("getMatchAllQueryData");
    search.search(
        SearchQuery.builder().build(),
        result -> {
          System.out.println("a: " + result.getSourceAsString());
        });

    System.out.println("getTermAndTermQueryData");
    search.search(
        SearchQuery.builder().clause(clause1).clause(clause2).build(),
        result -> {
          System.out.println("b: " + result.getSourceAsString());
        });

    System.out.println("getPhraseQueryData");
    search.search(
        SearchQuery.builder().clause(clause3).build(),
        result -> {
          System.out.println("c: " + result.getSourceAsString());
        });

    // Ingest single record
    System.out.println("\nIngestService response::: " + search.ingest(indexName, "default", json1));

    // Ingest batch of records
    System.out.println(
        "\nIngestService response::: "
            + search.ingest(indexName, "default", Arrays.asList(json2, json3, json4)));

    // Count
    System.out.println("getMatchAllQueryCount " + search.count(SearchQuery.builder().build()));

    // Delete
    System.out.println("delete by query " + search.deleteByQuery(indexName, "furby").getDeleted());

    System.out.println(
        "delete by query " + search.deleteByQuery(indexName, "blizzard").getDeleted());

    // Count records
    System.out.println("getMatchAllQueryCount " + search.count(SearchQuery.builder().build()));

    search.destroy();
  }
}
