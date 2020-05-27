package com.company.service;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;

/***
 * add delete by query plugin to Elastisearch
 * $ bin/plugin install delete-by-query
 */
public class DeleteService {
  Client client;
  String indexName;

  public DeleteService(Client client, String indexName) {
    this.client = client;
    this.indexName = indexName;
  }

  public DeleteResponse delete(String id) {
    return client.prepareDelete(indexName, "default", id).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();
  }

  public BulkByScrollResponse deleteByQuery(String description) {
    BulkByScrollResponse response =
      DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
        .filter(QueryBuilders.matchPhraseQuery("description", description))
        .source(indexName)
        .refresh(true)
        .get();

    return response;
  }
}
