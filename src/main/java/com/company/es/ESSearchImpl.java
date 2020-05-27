package com.company.es;

import com.company.common.ISearch;
import com.company.common.SearchQuery;
import com.company.common.SearchQueryClause;
import com.company.common.SearchResult;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class ESSearchImpl implements ISearch {

  protected String hostname;
  protected int port = 9300;
  protected String clusterName;
  protected List<String> indexes = new ArrayList();
  protected Client client;

  public ESSearchImpl(String hostname, int port, String clusterName, List<String> indexes) {
    this.hostname = hostname;
    this.port = port;
    this.clusterName = clusterName;
    this.indexes = indexes;

    Settings settings = Settings.builder()
      .put("cluster.name", clusterName).build();

    this.client = new PreBuiltTransportClient(settings)
      .addTransportAddress(new TransportAddress(new InetSocketAddress(hostname, port)));
  }

  @Override
  public void search(SearchQuery searchQuery, Consumer<SearchResult> consumer) {
    BoolQueryBuilder query = this.createBoolQuery(searchQuery);
    String indexName = indexes.get(0);

    SearchHit[] hits = client.prepareSearch(indexName).setQuery(query).execute().actionGet().getHits().getHits();

    if (hits != null) {
      for (SearchHit hit : hits) {
        SearchResult.SearchResultBuilder searchResult = SearchResult.builder();

        searchResult.contentId(hit.getId());
        searchResult.indexId(hit.getIndex());
        searchResult.type(hit.getType());
        searchResult.score(hit.getScore());
        searchResult.sourceAsString(hit.getSourceAsString());

        consumer.accept(searchResult.build());
      }
    }
  }

  @Override
  public long count(SearchQuery searchQuery) {
    BoolQueryBuilder query = this.createBoolQuery(searchQuery);
    String indexName = indexes.get(0);

    long count = client.prepareSearch(indexName).setQuery(query).setSize(0).execute().actionGet().getHits().getTotalHits();
    return count;
  }

  @Override
  public SearchResult get(String indexName, String id) {
    GetResponse getResponse = client.prepareGet(indexName, "default", id).get();

    if (getResponse.isExists()) {
      SearchResult.SearchResultBuilder searchResult = SearchResult.builder();

      searchResult.contentId(getResponse.getId());
      searchResult.indexId(getResponse.getIndex());
      searchResult.type(getResponse.getType());
      searchResult.sourceAsString(getResponse.getSourceAsString());

      return searchResult.build();
    } else {
      return null;
    }
  }

  @Override
  public IndexResponse ingest(String indexName, String type, String doc) {
    return client.prepareIndex(indexName, type).setSource(doc, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();
  }

  @Override
  public BulkResponse ingest(String indexName, String type, List<String> docs) {
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    docs.forEach(doc -> bulkRequest.add(client.prepareIndex(indexName, type).setSource(doc, XContentType.JSON)).get());

    return bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();
  }

  @Override
  public DeleteResponse delete(String indexName, String id) {
    return client.prepareDelete(indexName, "default", id).get();
  }

  @Override
  public BulkByScrollResponse deleteByQuery(String indexName, String description) {
    BulkByScrollResponse response =
      DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
        .filter(QueryBuilders.matchPhraseQuery("description", description))
        .source(indexName)
        .refresh(true)
        .get();

    return response;
  }

  private BoolQueryBuilder createBoolQuery(SearchQuery searchQuery) {
    BoolQueryBuilder rootQuery = QueryBuilders.boolQuery();

    if (searchQuery.getClauses().size() > 0) {
      for (SearchQueryClause clause : searchQuery.getClauses()) {
        SearchQueryClause.ClauseType clauseType = clause.getType();
        String fieldName = clause.getFieldName();
        String text = clause.getText();

        if (clauseType == SearchQueryClause.ClauseType.ALL) {
          QueryBuilder query = QueryBuilders.matchAllQuery();
          rootQuery.must(query);
        } else if (clauseType == SearchQueryClause.ClauseType.MATCH) {
          QueryBuilder query = QueryBuilders.matchQuery(fieldName, text);
          rootQuery.must(query);
        } else if (clauseType == SearchQueryClause.ClauseType.MATCH_TERM) {
          QueryBuilder query = QueryBuilders.termQuery(fieldName, text);
          rootQuery.must(query);
        } else if (clauseType == SearchQueryClause.ClauseType.MATCH_PHRASE) {
          QueryBuilder query = QueryBuilders.matchPhraseQuery(fieldName, text);
          rootQuery.must(query);
        } else if (clauseType == SearchQueryClause.ClauseType.MULTI_MATCH) {
          String[] fieldNames = fieldName.split(",");
          QueryBuilder query = QueryBuilders.multiMatchQuery(text, fieldNames);
          rootQuery.must(query);
        } else {
          throw new IllegalArgumentException("unknown search type");
        }
      }
    } else {
      QueryBuilder query = QueryBuilders.matchAllQuery();
      rootQuery.must(query);
    }
    System.out.println(rootQuery.toString());

    return rootQuery;
  }

  @Override
  public void destroy() {
    if (client != null) {
      client.close();
    }
  }
}
