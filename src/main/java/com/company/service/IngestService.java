package com.company.service;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;

public class IngestService {
  Client client;
  String indexName;

  public IngestService(Client client, String indexName) {
    this.client = client;
    this.indexName = indexName;
  }

  public IndexResponse ingest(String type, String doc) {
    return client.prepareIndex(indexName, type).setSource(doc, XContentType.JSON).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();
  }

  public BulkResponse ingest(String type, List<String> docs) {
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    docs.forEach(doc -> bulkRequest.add(client.prepareIndex(indexName, type).setSource(doc, XContentType.JSON)).get());

    return bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL).get();
  }
}
