package com.company.app;

import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class AppTest {

  @Before
  public void setup() {
    String clusterName = "elasticsearch";
    String indexName = "products";

    List<String> indexes = new ArrayList<String>();
    indexes.add(indexName);

    Settings settings = Settings.builder()
      .put("cluster.name", clusterName).build();
  }

  /**
   * Trivial Test
   */
  @Test
  public void testApp() {
    assertTrue(true);
  }
}
