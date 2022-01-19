package com.company.search.query;

import com.aisera.common.*;
import com.aisera.common.config.SystemConfig.SystemConfigMapper.SearchType;
import com.aisera.common.search.SearchUtils;
import com.aisera.common.utils.AssetUtils;
import com.aisera.common.utils.Country;
import com.aisera.common.utils.ProtoUtils;
import com.aisera.common.utils.WordUtils;
import com.aisera.nlp.wordnet.WordBoundary;
import com.aisera.tenant.client.BotClient;
import com.aisera.tenant.dto.BotDTO;
import com.aisera.userprofile.client.UpsGrpcClient;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public class SearchQuery {
  private final QueryNLPObject queryNLPObject;
  @Getter private final List<String> indexes;
  @Setter @Getter
  private List<SearchType> searchTypes;

  private String botLanguage;
  private Map<String, List<String>> synonymsMap;

  // user profile related
  private final AtomicBoolean isUserProfileLoaded = new AtomicBoolean();
  private String assetDesc;
  private List<String> locations;
  private List<String> countries;
  private String department;

  private static final BotClient botClient = new BotClient();
  private static final UpsGrpcClient upsClient = new UpsGrpcClient();

  public SearchQuery(QueryNLPObject queryNLPObject, List<String> indexes) {
    this.queryNLPObject = queryNLPObject;
    this.indexes = indexes;
  }

  public static SearchQuery searchQueryForContentFetching(String tenantId, List<String> indexes, String contentIds) {
    QueryObject queryObject = QueryObject.builder()
      .tenantId(tenantId)
      .build();
    QueryNLPObject queryNLPObject = new QueryNLPObject(queryObject, contentIds);
    return new SearchQuery(queryNLPObject, indexes);
  }
  /**
   * Getters
   */

  // namespace is same as TenantId
  public String getNamespace() {
    return queryNLPObject.getQueryObject().getTenantId();
  }

  public Collection<String> getDefaultConfigEntities() {
    return queryNLPObject.getEntitiesForDefaultConfig();
  }

  public Collection<String> getSPANEntities() {
    return queryNLPObject.getSpanEntitiesForSearch();
  }

  public Collection<String> getNEREntities() {
    return queryNLPObject.getEntitiesForSearch();
  }

  public Collection<String> getSuperIntents() {
    return queryNLPObject.getSuperIntents();
  }

  public Collection<String> getPredEntityUtterance() {
    return queryNLPObject.getPredEntityUtterance();
  }

  public String getQuestionText() {
    return queryNLPObject.getQueryStr();
  }

  public List<String> getTokens() {
    return queryNLPObject.getAnalyzedTokens();
  }

  public List<Map<String, String>> getEntityCategories() {
    return extractOntologyMap(queryNLPObject.getQueryObject().getQuestion().getEntitiesList());
  }

  public List<SourceChannelType> getSearchItemTypes() {
    return queryNLPObject.getQueryObject().getSearchItemTypes();
  }

  public String getBotLanguage() {
    if (botLanguage == null) {
      QueryObject queryObject = queryNLPObject.getQueryObject();
      String tenantId = queryObject.getTenantId();
      Long botId = queryObject.getBotId();
      botLanguage = SearchUtils.DEFAULT_LANGUAGE_CODE;
      BotDTO botById = botClient.getBotById(tenantId, botId);
      if (botById != null && botById.getLanguage() != null && !botById.getLanguage().equals(SearchUtils.DEFAULT_LANGUAGE_CODE)) {
        botLanguage = botById.getLanguage();
      }
    }
    return botLanguage;
  }

  public String getSynonymText() {
    if (queryNLPObject.getQueryObject().getQueryConfig().isSynonymSearch()) {
      return SearchUtils.getSynonymsAsText(getSynonymsMap(), SearchUtils.topKSynonym).trim();
    }
    return "";
  }

  public QueryObject getQueryObject() {
    return queryNLPObject.getQueryObject();
  }

  private Map<String, List<String>> getSynonymsMap() {
    if (synonymsMap == null) {
      synonymsMap = new HashMap<>();
      if (SearchUtils.getKeyAsBoolean(getQueryObject().getTenantId(), "synonymSearch")) {

        // collect entities and its associated synonyms
        Sentence.Builder builder = Sentence.newBuilder(queryNLPObject.getQueryObject().getQuestion().getSentence());
        final Optional<WordBoundary> wbInstance = WordBoundary.getInstance();

        // populate synonyms
        ProtoUtils.iterateOnWords(builder, wordBuilder -> wbInstance.ifPresent(boundary -> {
          // forced map for all type=JJ* to NN
          switch(wordBuilder.getType()) {
            case JJ:
            case JJR:
            case JJS:
              wordBuilder.setType(WordTag.NN);
              break;
          }
          // get synonyms
          // a) skip compound synonyms
          // b) skip term not present in ES index
          if (WordUtils.WordTagUtils.isNoun(wordBuilder.getType())) {
            synonymsMap.put(wordBuilder.getWord(), boundary.getSynonyms(wordBuilder.build(), true)
              .getSynonymsList().stream()
              .filter(syn -> (!syn.contains("-") && !syn.contains("_")))
              .collect(Collectors.toList()));
          }
        }));
        log.info("Found synonyms = {}", SearchUtils.getSynonymsAsText(synonymsMap, SearchUtils.topKSynonym));
      }
      else {
        log.info("Synonym search is disabled.");
      }

    }

    return synonymsMap;
  }


  public String getAssetDesc() {
    loadUserProfileResponse();
    return assetDesc;
  }

  public List<String> getLocations() {
    loadUserProfileResponse();
    return locations;
  }

  public List<String> getCountries() {
    loadUserProfileResponse();
    return countries;
  }

  public String getDepartment() {
    loadUserProfileResponse();
    return department;
  }


  ///////


  private void loadUserProfileResponse() {
    if (isUserProfileLoaded.compareAndSet(false, true)) {
      String userEmail = getQueryObject().getUserEmail();
      String tenantId = getQueryObject().getTenantId();
      if (StringUtils.isEmpty(userEmail)) {
        return;
      }
      UserProfileResponse userProfileResponse = upsClient.getUserProfile(tenantId, userEmail);
      if (!userProfileResponse.hasUserProfile()) {
        log.error("Failed to get user profile for tenant: {}. Error message: {}", tenantId, userProfileResponse.getStatusMessage());
        return;
      }

      UserProfile userProfile = userProfileResponse.getUserProfile();
      List<String> userModels = Optional.of(userProfile.getAssetsList())
        .filter(allAssets -> !allAssets.isEmpty())
        .map(all -> all.stream().map(AssetInfo::getModel).collect(Collectors.toList()))
        .orElse(null);
      assetDesc = String.join(" ", AssetUtils.expandedAssetName(AssetUtils.findAsset(userModels)));

      locations = new ArrayList<>();
      countries = new ArrayList<>();
      Address address = userProfile.getWorkInfo().getLocation().getAddress();
      if (StringUtils.isNotEmpty(address.getCity())) {
        locations.add(address.getCity());
      }
      if (StringUtils.isNotEmpty(address.getState())) {
        locations.add(address.getState());
      }
      if (StringUtils.isNotEmpty(address.getCountry())) {
        locations.add(Country.getNormalizedName(address.getCountry()));
        countries.add(Country.getNormalizedName(address.getCountry()));
      }

      department = null;
      if (StringUtils.isNotEmpty(userProfile.getWorkInfo().getDepartment().getName())) {
        department = userProfile.getWorkInfo().getDepartment().getName();
      }
      log.info("User profile country: {}", String.join(", ", countries));
      log.info("User profile location: {}", String.join(", ", locations));
      log.info("User profile department: {}", department);
    }
  }


  public String[] getIndexesAsArray() {
    return this.indexes.toArray(new String[0]);
  }

  public static IndexSearchGraph.SearchField buildSearchField(String name) {
    return buildSearchField(name, 1.0f);
  }

  public static IndexSearchGraph.SearchField buildSearchField(String name, float boost) {
    return buildSearchField(name, boost, SearchUtils.DEFAULT_LANGUAGE_CODE);
  }

  public static IndexSearchGraph.SearchField buildSearchField(String name, String botLanguage) {
    return buildSearchField(name, 1.0f, botLanguage);
  }

  public static IndexSearchGraph.SearchField buildSearchField(String name, float boost, String botLanguage) {
    IndexSearchGraph.SearchField field = new IndexSearchGraph.SearchField();
    if (!SearchUtils.DEFAULT_LANGUAGE_CODE.equals(botLanguage)) {
      name = name.concat(SearchUtils.SUFFIX_FOR_LANGUAGE_SUPPORT);
    }
    field.setName(name);
    field.setBoost(boost);
    return field;
  }

  // collect Categories into map to be used by search query
  private List<Map<String, String>> extractOntologyMap(List<KGEntity> entities) {
    List<Map<String, String>> categoryEntityMap = new ArrayList<>();
    entities.forEach(kgEntity -> kgEntity.getEntityCategoryList()
      .forEach(cat -> collectCategory("entities.entityCategory", cat, categoryEntityMap)));
    return categoryEntityMap;
  }

  private void collectCategory(String label, EntityCategory category, List<Map<String, String>> categoryEntityMap) {
    // add current category
    Map<String, String> item = new HashMap<>();
    item.put(label + ".name", category.getName());
    categoryEntityMap.add(item);

    // recursively add items from category list
    category.getEntityCategoryList().forEach(cat -> collectCategory(label + ".entityCategory", cat, categoryEntityMap));
  }
}
