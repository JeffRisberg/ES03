package com.company.service;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class CountService {

  Client client;
  String indexName;

  public CountService(Client client, String indexName) {
    this.client = client;
    this.indexName = indexName;
  }

  public long getMatchAllQueryCount() {
    QueryBuilder query = QueryBuilders.matchAllQuery();
    System.out.println("getMatchAllQueryCount query => " + query.toString());

    long count = client.prepareSearch(indexName).setQuery(query).setSize(0).execute().actionGet().getHits().getTotalHits();

    return count;
  }

  public long getBoolQueryCount(String text) {
    QueryBuilder query = QueryBuilders.boolQuery().must(
      QueryBuilders.termQuery("name", "lobster")
    ).must(QueryBuilders.termQuery("description", text));
    System.out.println("getBoolQueryCount query =>" + query.toString());

    long count = client.prepareSearch(indexName).setQuery(query).setSize(0).execute().actionGet().getHits().getTotalHits();

    return count;
  }

  public long getPhraseQueryCount(String text) {
    QueryBuilder query = QueryBuilders.matchPhraseQuery("name", text);
    System.out.println("getPhraseQueryCount query =>" + query.toString());

    long count = client.prepareSearch(indexName).setQuery(query).setSize(0).execute().actionGet().getHits().getTotalHits();

    return count;
  }
}
