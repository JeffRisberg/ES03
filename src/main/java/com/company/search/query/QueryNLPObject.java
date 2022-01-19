package com.company.search.query;

import com.aisera.common.Question;
import com.aisera.common.ResourceLocator;
import com.aisera.common.config.SystemConfigUtil;
import com.aisera.common.utils.ProtoUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.aisera.modelSDK.impl.ModelSDK.DEFAULT_MODEL_REQUEST_CHAR_LIMIT;
import static com.aisera.modelSDK.impl.ModelSDK.MODEL_REQUEST_CHAR_LIMIT;
import static com.aisera.search.query.impl.ElasticSearchImpl.*;

@Slf4j
public class QueryNLPObject {

  private com.aisera.search.query.EntityUtils.DiscoveredPredicates discoveredPredicates;
  private Set<String> allPreds;
  private Set<com.aisera.search.query.EntityUtils.AnnotatedEntity> filteredNounEntities;
  private Set<com.aisera.search.query.EntityUtils.AnnotatedEntity> filteredNEREntities;
  private Set<com.aisera.search.query.EntityUtils.AnnotatedEntity> expandedNER2Entities;
  private Set<String> entityUtterancesForSPAN;
  private Set<String> spanEntitiesForSearch;
  private Set<String> entitiesForSearch;
  private Set<String> superIntents;
  private Set<String> predEntityUtterance;
  private Set<String> entitiesForDefaultConfig;
  private List<String> analyzedTokens;

  @Getter private final com.aisera.search.query.QueryObject queryObject;
  @Getter private final String queryStr;
  static int maxCount = ResourceLocator.getResourceAsInt(SEARCH_SERVER_QUERYNLP_MAX_COUNT).orElse(MAX_COUNT_DEFAULT);

  public QueryNLPObject(com.aisera.search.query.QueryObject queryObject) {
    this(queryObject, null);
  }

  // When queryStr is present skip all NLP. This is used for content fetching
  public QueryNLPObject(com.aisera.search.query.QueryObject queryObject, String queryStr) {
    this.queryObject = queryObject;
    if (queryStr == null) {
      Question question = queryObject.getQuestion();
      String queryStrTmp = (StringUtils.isBlank(question.getSpellCheckedText()) ? question.getText() : question.getSpellCheckedText());
      this.queryStr = StringUtils.isNotBlank(queryStrTmp) ? applyUtteranceMap(queryObject.getQueryConfig().getSynset(), queryStrTmp) : queryObject.getQueryStr();
    } else {
      this.queryStr = queryStr;
    }
  }


  /**
   * Lazy Getters
   */
  public Set<String> getSpanEntitiesForSearch() {
    if (spanEntitiesForSearch == null) {
      spanEntitiesForSearch = truncatedSet(
        com.aisera.search.query.EntityUtils.genSPANEntitiesForSearch(queryObject.getQueryConfig(), getExpandedNER2Entities(), getEntityUtterancesForSPAN()),
        maxCount, "spanEntitiesForSearch");
      log.debug("SPAN ENTITIES for entity search: {}", String.join(" \n", spanEntitiesForSearch));
    }

    return spanEntitiesForSearch;
  }

  public Set<String> getEntitiesForSearch() {
    if (entitiesForSearch == null) {
      // regular entities for advance entity search, entity driven search
      entitiesForSearch = truncatedSet(
        com.aisera.search.query.EntityUtils.genRegularEntitiesForSearch(queryObject.getQueryConfig(), getExpandedNER2Entities(), getFilteredNEREntities()),
        maxCount, "regularEntitiesForSearch");
      log.debug("Regular ENTITIES for entity search: {}", String.join(" \n", entitiesForSearch));
    }

    return entitiesForSearch;
  }

  public Set<String> getSuperIntents() {
    if (superIntents == null) {
      // collect superIntents
      if (queryObject.getQueryConfig().isMatchSuperIntent()) {
        superIntents = truncatedSet(com.aisera.search.query.EntityUtils.getSuperIntents(queryObject.getQuestion(), getFilteredNEREntities()), maxCount, "superIntents");
        log.info("allSuperIntents: {}", String.join(", ", superIntents));
      } else {
        superIntents = new HashSet<>();
      }
    }

    return superIntents;
  }

  public Set<String> getPredEntityUtterance() {
    // L1/L2 utterances Generated utterances for SPAN(PRED/ENTITY) reuse genEntityUtterancesForSPAN
    if (predEntityUtterance == null) {
      predEntityUtterance = truncatedSet(
        com.aisera.search.query.EntityUtils.genPredEntityUtterance(queryObject.getQueryConfig(), getAllPreds(), getEntityUtterancesForSPAN()),
        maxCount, "predEntityUtterance");
      log.debug("PRED+ENTITY utterances: {}", String.join(" \n", predEntityUtterance));
    }

    return predEntityUtterance;
  }

  public Set<String> getEntitiesForDefaultConfig() {
    if (entitiesForDefaultConfig == null) {
      Set<com.aisera.search.query.EntityUtils.AnnotatedEntity> predWAEntities = getQueryEntities(queryObject);
      entitiesForDefaultConfig = truncatedSet(com.aisera.search.query.EntityUtils.toRootEntitiesOnly(predWAEntities, com.aisera.search.query.EntityUtils.EntityNameSpace.PRED_WA),
        maxCount, "entitiesForDefaultConfig");
    }

    return entitiesForDefaultConfig;
  }

  public List<String> getAnalyzedTokens() {
    if (analyzedTokens == null) {
      // analyze question text and collect tokens
      analyzedTokens = getAnalyzedTokens(queryStr, queryObject.getTenantId());
    }

    return analyzedTokens;
  }

  private Set<String> getAllPreds() {
    if (allPreds == null) {
      allPreds = truncatedSet(discoveredPredicates.getAllPreds(), maxCount, "allPreds");
      log.debug("PRED for PRED+ENTITY expansion: {}", String.join(", ", allPreds));
    }

    return allPreds;
  }

  private com.aisera.search.query.EntityUtils.DiscoveredPredicates getDiscoveredPredicates() {
    if (discoveredPredicates == null) {
      // get PRED for SPAN search
      discoveredPredicates = com.aisera.search.query.EntityUtils.getAllPreds(getQueryObject().getQuestion());
    }

    return discoveredPredicates;
  }

  private Set<com.aisera.search.query.EntityUtils.AnnotatedEntity> getFilteredNounEntities() {
    if (filteredNounEntities == null) {
      // build Noun Entities
      filteredNounEntities =
        com.aisera.search.query.EntityUtils.getNounEntities(queryObject.getQueryConfig(), queryObject.getQuestion(),
          getDiscoveredPredicates().getWaPreds(), getFilteredNEREntities(), queryObject.getQueryConfig().getEntityExcludeSet(),
          getAllPreds());
      log.debug("Noun entities after exclusion: {}", com.aisera.search.query.EntityUtils.toStringRootEntitiesOnly(filteredNounEntities));

    }

    return filteredNounEntities;
  }

  private Set<com.aisera.search.query.EntityUtils.AnnotatedEntity> getFilteredNEREntities() {
    if (filteredNEREntities == null) {
      // get entities and check query meaningfulness
      String entityParentClassFilter = SystemConfigUtil.getInstance().getKeyAsString(queryObject.getTenantId(), "entityClassFilterRegex", null);
      Pattern pattern = null;
      try {
        if (StringUtils.isNotBlank(entityParentClassFilter)) {
          pattern = Pattern.compile(entityParentClassFilter);
        }
      } catch (Exception e) {
        log.error("Failed to create regex for entityClassFilterRegex", e);
      }
      filteredNEREntities = new LinkedHashSet<>(truncatedSet(
        com.aisera.search.query.EntityUtils.getNEREntities(queryObject.getQuestion(), queryObject.getQueryConfig().getEntityExcludeSet(),
          getAllPreds(), pattern), maxCount,  "filteredNEREntities"));
      log.debug("NER entities after exclusion: {}", com.aisera.search.query.EntityUtils.toStringRootEntitiesOnly(filteredNEREntities));

    }
    return filteredNEREntities;
  }

  private Set<com.aisera.search.query.EntityUtils.AnnotatedEntity> getExpandedNER2Entities() {
    if (expandedNER2Entities == null) {
      // Entities for PRED+Entity expansion
      expandedNER2Entities = com.aisera.search.query.EntityUtils.getNERPlusNounEntities(queryObject.getQueryConfig(), getFilteredNEREntities(),
        getFilteredNounEntities());
    }
    return expandedNER2Entities;
  }

  private Set<String> getEntityUtterancesForSPAN() {
    if (entityUtterancesForSPAN == null) {
      //generate utterances for entity only
      entityUtterancesForSPAN =  com.aisera.search.query.EntityUtils.genEntityUtterancesForSPAN(getExpandedNER2Entities());
    }

    return entityUtterancesForSPAN;
  }

  /**
   * Utilities
   */

  private Set<com.aisera.search.query.EntityUtils.AnnotatedEntity> getQueryEntities(com.aisera.search.query.QueryObject queryObject) {
    Collection<String> finalEntities;

    Question queryQuestion = queryObject.getQuestion();
    com.aisera.search.query.QueryConfig queryConfig = queryObject.getQueryConfig();

    if (queryConfig.isOnlyPredObjEntities()) {
      finalEntities =  ProtoUtils.getEntities(queryQuestion.getQuadsList());
      log.debug("allEntities by PredObj model: {}", String.join(", ", finalEntities));
    } else {
      // default case
      finalEntities = ProtoUtils.getEntities(queryQuestion);
      log.debug("allEntities by PredObj+WordAnnotation model: {}", String.join(", ", finalEntities));
    }

    LinkedHashSet<com.aisera.search.query.EntityUtils.AnnotatedEntity> allEntities = new LinkedHashSet<>();
    if (CollectionUtils.isNotEmpty(finalEntities)) {
      finalEntities.stream()
        .map(item -> new com.aisera.search.query.EntityUtils.AnnotatedEntity(item, com.aisera.search.query.EntityUtils.EntityNameSpace.PRED_WA, new ArrayList<>()))
        .forEach(allEntities::add);
    }

    // apply include and exclude filter
    return com.aisera.search.query.EntityUtils.filterEntityByExcludeList(queryConfig.getEntityExcludeSet(), allEntities);
  }

  /**
   * Replace text in user question using Utterance Map definition. For example
   * Definition data:
   *   "i want to" -> "order"
   *   "i would like to" -> "order"
   * User question:
   *   "I  would like to   order new mouse"
   * Mapped Question:
   *   "order new mouse"
   * @param question
   * @return
   */
  private String applyUtteranceMap(com.aisera.search.query.Synset synset, String question) {
    try {
      if (synset.getUtterancePreprocessMap() == null || StringUtils.isBlank(question)) {
        log.info("Utterance Map is not available. Will keep original question.");
        return question;
      }
      // fix question so that we have only one space separator between tokens
      String[] tokens = question.split(" ");
      question = Arrays.stream(tokens)
        .map(token -> StringUtils.trim(token.toLowerCase()))
        .collect(Collectors.joining(" "));
      question = question.toLowerCase();
      for (Map.Entry<String, String> entry: synset.getUtterancePreprocessMap().entrySet()) {
        question = question.replaceAll(entry.getKey(), entry.getValue());
      }
      log.info("Question text after applying utterance mapping: {}", question);
      return question;
    } catch (Exception e) {
      log.error("Failed to map utternace:", e);
      return question;
    }
  }

  private List<String> getAnalyzedTokens(String queryStr, String tenantId) {
    // analyze question text and collect tokens
    List<String> analyzedTokens = new ArrayList<>();
    try {
      if(queryStr.length() > ResourceLocator.getResourceAsInt(MODEL_REQUEST_CHAR_LIMIT)
        .orElse(DEFAULT_MODEL_REQUEST_CHAR_LIMIT)) {
        return analyzedTokens;
      }
      //TODO change default setting of Indexes have max limit -> Generating excessive amount of tokens may cause a node to run out of memory.
      //https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-analyze.html
      com.aisera.search.query.SearchClient.getClient().analyzeText(queryStr, analyzedTokens::add);
    } catch (Exception e) {
      log.warn("Error analyzing search text, ignored ({})", e.getMessage());
      return analyzedTokens;
    }

    // check for too many terms in user utterance
    int numTermsAllowed = SystemConfigUtil.getInstance().getKeyAsInteger(tenantId, "maxTermsForSearch", 10);
    if (analyzedTokens.size() > numTermsAllowed) {
      String msg = "User question is too verbose. ["+analyzedTokens.size()+"] tokens found from search analyzer "
        + "but only ["+numTermsAllowed+"] allowed. Tokens from Elastic=[" + String.join(", ", analyzedTokens);
      log.warn(msg);
      analyzedTokens = analyzedTokens.subList(0, numTermsAllowed);
    }
    log.info("spellFixedQuestion: {}, tokens: {}", queryStr, String.join(", ", analyzedTokens));
    return analyzedTokens;
  }
}
