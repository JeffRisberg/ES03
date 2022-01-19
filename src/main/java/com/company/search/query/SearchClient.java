package com.company.search.query;

import com.aisera.search.query.impl.ElasticSearchImpl;

/**
 * Created by kobiruskin on 7/12/17.
 */
public class SearchClient {

  private static ISearch client = null;

  public static synchronized ISearch getClient() throws Exception {
    // return the right impl based on the deployment
    if (client == null) {
      System.setProperty("es.set.netty.runtime.available.processors", "false");
      client = new ElasticSearchImpl();
      client.init();
    }

    return client;
  }
}
