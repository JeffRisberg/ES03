package com.company.service;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;

public class DataService {
  Client client;
  String indexName;

  public DataService(Client client, String indexName) {
    this.client = client;
    this.indexName = indexName;
  }

  public List<String> getMatchAllQueryData() {
    QueryBuilder query = QueryBuilders.matchAllQuery();
    System.out.println("getMatchAllQueryData query => " + query.toString());

    SearchHit[] hits = client.prepareSearch(indexName).setQuery(query).execute().actionGet().getHits().getHits();

    List<String> list = new ArrayList<String>();
    for (SearchHit hit : hits) {
      list.add(hit.getSourceAsString());
    }
    return list;
  }

  public List<String> getBoolQueryData(String text1, String text2) {
    QueryBuilder query = QueryBuilders.boolQuery().must(
      QueryBuilders.termQuery("name", text1)
    ).must(QueryBuilders.termQuery("description", text2));
    System.out.println("getBoolQueryData query => " + query.toString());

    SearchHit[] hits = client.prepareSearch(indexName).setQuery(query).execute().actionGet().getHits().getHits();

    List<String> list = new ArrayList<String>();
    for (SearchHit hit : hits) {
      list.add(hit.getSourceAsString());
    }
    return list;
  }

  public List<String> getPhraseQueryData(String text) {
    QueryBuilder query = QueryBuilders.matchPhraseQuery("name", text);
    System.out.println("getPhraseQueryData query => " + query.toString());

    SearchHit[] hits = client.prepareSearch(indexName).setQuery(query).execute().actionGet().getHits().getHits();

    List<String> list = new ArrayList<String>();
    for (SearchHit hit : hits) {
      list.add(hit.getSourceAsString());
    }
    return list;
  }
}
