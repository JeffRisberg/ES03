package com.company.common;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

import java.util.List;
import java.util.function.Consumer;

public interface ISearch {

  public void search(SearchQuery searchQuery, Consumer<SearchResult> consumer);

  public long count(SearchQuery searchQuery);

  public SearchResult get(String indexName, String id);

  public IndexResponse ingest(String indexName, String type, String doc);

  public BulkResponse ingest(String indexName, String type, List<String> docs);

  public DeleteResponse delete(String index, String id);

  public BulkByScrollResponse deleteByQuery(String indexName, String description);

  public void destroy();
}
