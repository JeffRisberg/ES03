package com.company.search.query;

import com.aisera.common.config.SystemConfigUtil;
import com.aisera.common.search.SearchUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

@AllArgsConstructor
@Builder
@Data
@ToString
public class QueryConfig {

  public static final int DEFAULT_MAX_SEARCH_SIZE = 100;

  int maxSearchResults;
  boolean skipBOL;
  float normalizedScoreThreshold;

  // entity related config
  boolean onlyEntityMatchedItems;
  boolean entityMustMatch;
  boolean matchSuperIntent;

  int maxTermsForSearch;
  int maxSCSearchResults;

  @Builder.Default
  Set<String> entityExcludeSet = new HashSet<>();

  float ontologySearchBoostByLevel;
  int minTermsForSpanSearch;
  int phraseQuerySlop;
  boolean synonymSearch;
  String invalidTokensInEntity;
  boolean expandUtteranceByNERSynset;
  int maxPredEntityUtterances;
  boolean onlyPredObjEntities;

  Synset synset;

  private List<Integer> spanQuerySlops;
  private List<Integer> predEntitySlops;



  public static QueryConfig loadFromSystemConfig(String tenantId, com.aisera.common.SearchType searchType) {
    int defaultMaxSearchResults = 5;
    String maxSearchResultsKey = "maxSearchResults";
    boolean skipBOL = false;
    if (searchType.equals(com.aisera.common.SearchType.Enterprise)) {
      defaultMaxSearchResults = DEFAULT_MAX_SEARCH_SIZE;
      maxSearchResultsKey = "enterpriseMaxResults";
      skipBOL = SystemConfigUtil.getInstance().getKeyAsBoolean(tenantId, "skipBOL", false);
    }

    SystemConfigUtil systemConfigUtil = SystemConfigUtil.getInstance();

    List<Integer> spanQuerySlops = SearchUtils.getSpanQuerySLOP(tenantId);
    List<Integer> predEntitySlops = new ArrayList<>(spanQuerySlops);
    predEntitySlops.add(SearchUtils.getKeyAsInteger(tenantId, "predEntityQuerySlop"));

    return new QueryConfigBuilder()
      .maxSearchResults(systemConfigUtil.getKeyAsInteger(tenantId, maxSearchResultsKey, defaultMaxSearchResults))
      .skipBOL(skipBOL)
      .normalizedScoreThreshold(systemConfigUtil.getKeyAsFloat(tenantId, "normalizedScoreThreshold", 0.f))
      .onlyEntityMatchedItems(systemConfigUtil.getKeyAsBoolean(tenantId, "onlyEntityMatchedItems", false))
      .entityMustMatch(systemConfigUtil.getKeyAsBoolean(tenantId, "entityMustMatch", false))
      .maxTermsForSearch(systemConfigUtil.getKeyAsInteger(tenantId, "maxTermsForSearch", 10))
      .matchSuperIntent(systemConfigUtil.getKeyAsBoolean(tenantId, "matchSuperIntent", false))
      .maxSCSearchResults(systemConfigUtil.getKeyAsInteger(tenantId, "maxSCSearchResults", 5))
      .entityExcludeSet(loadEntityExcludeSet(tenantId))
      .ontologySearchBoostByLevel(SearchUtils.getKeyAsFloat(tenantId, "ontologySearchBoostByLevel"))
      .minTermsForSpanSearch(SearchUtils.getKeyAsInteger(tenantId, "minTermsForSpanSearch"))
      .phraseQuerySlop(SearchUtils.getKeyAsInteger(tenantId, "phraseQuerySlop"))
      .synonymSearch(SearchUtils.getKeyAsBoolean(tenantId, "synonymSearch"))
      .invalidTokensInEntity(systemConfigUtil.getKeyAsString(tenantId, "invalidTokensInEntity", ""))
      .expandUtteranceByNERSynset(systemConfigUtil.getKeyAsBoolean(tenantId, "expandUtteranceByNERSynset", true))
      .maxPredEntityUtterances(systemConfigUtil.getKeyAsInteger(tenantId, "maxPredEntityUtterances", 10))
      .onlyPredObjEntities(systemConfigUtil.getKeyAsBoolean(tenantId, "onlyPredObjEntities", false))
      .synset(Synset.getSynset(tenantId))
      .spanQuerySlops(spanQuerySlops)
      .predEntitySlops(predEntitySlops)
      .build();
  }

  private static Set<String> loadEntityExcludeSet(String tenantId) {
    // if valid entities is empty, return original set
    String val = SystemConfigUtil.getInstance().getKeyAsString(tenantId, "invalidEntities", "");
    val = StringUtils.normalizeSpace(val);
    if(StringUtils.isBlank(val))
      return new HashSet<>();

    String[] entities = val.split(",");
    Set<String> entityExcludeSet = Arrays.stream(entities)
      .filter(StringUtils::isNotBlank)
      .map(item -> item.trim().toLowerCase())
      .collect(Collectors.toSet());

    return entityExcludeSet;
  }

  public int getMaxSearchResults() {
    return Math.max(maxSearchResults, 1);
  }
}
