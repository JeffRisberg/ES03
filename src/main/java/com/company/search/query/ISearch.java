package com.company.search.query;

import com.aisera.search.common.SearchResultRecord;

import java.util.List;
import java.util.function.Consumer;

public interface ISearch {

  void init() throws Exception;

  /**
   * @param text
   * @param consumer
   */
  void analyzeText(String text, Consumer<String> consumer)
    throws Exception;


  // Fetch multiple Content ids while applying any filter present in the graph, eg FILTER_BY_COUNTRY
  void searchUsingMultipleContentIds(String tenantId, IndexSearchGraph graph, Consumer<SearchResultRecord> consumer);

  /**
   *
   * @param graph
   * @param consumer
   */
  void search(IndexSearchGraph graph, Consumer<SearchResultRecord> consumer, String layer);

  /**
   *
   * @param graph
   * @param consumer
   */
  void enterpriseSearch(IndexSearchGraph graph, Consumer<SearchResultRecord> consumer, String layer);


  void destroy() throws Exception;

  /**
   * Deletes a document from index
   * @param name index name
   * @param type document type
   * @param id document id
   */
  boolean deleteByDocId(String name, String type, String id) throws Exception;

  /**
   * Logically delete all items (by setting isArchived=true) from index which matches one of the sourceURIs
   * @param tenantId
   * @param dsId
   * @param sourceURIs list of URI to delete
   * @return
   * @throws Exception
   */
  boolean deleteBySourceURI(String tenantId, long dsId, List<String> sourceURIs, String retiredBy) throws Exception;

  /**
   * Logically delete all items (by setting isArchived=true) from index which matches one of the documentKeys
   * @param tenantId
   * @param dsId
   * @param documentKeys
   * @param retiredBy
   * @return
   * @throws Exception
   */
  boolean deleteByDocumentKey(String tenantId, long dsId, List<Long> documentKeys, String retiredBy) throws Exception;


  /**
   * Deletes all tickets from given indexName
   * @param tenantId
   * @param indexName
   * @return
   */
  long deleteTickets(String tenantId, String indexName) throws Exception;

  /**
   * use ES match query to determine if term is present in given indexes or not
   * @param tenantId
   * @param term
   * @param indexes
   * @return
   */
  boolean isTermPresent(String tenantId, String term, String[] indexes);

  /**
   * gets the count of all valid documents.
   * @param tenantId
   * @param dsId
   * @return
   * @throws Exception
   */
  long validDocCount(String tenantId, long dsId) throws Exception;

}
