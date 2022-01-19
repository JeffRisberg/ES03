package com.company.search.query;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class to help get Entities from model
 */
@Slf4j
public class EntityUtils {

  public static final String KGNER_TYPE_ENTITY = "Entity";
  private static final int MAX_NOUN_SIZE = 30;
  private static final int MAX_SYNSET_SIZE = 10;

  public enum EntityNameSpace {
    NER,
    NOUN,
    NER_NOUN,
    PRED_WA
  }

  @Getter @Setter
  public static class AnnotatedEntity {
    private String entity; // holds entity name
    private EntityNameSpace namespace;
    private String nerSubEntity; // only for NER_NOUN. Holds NER entity value.
    private String precedingNounEntity; // only for NER_NOUN. All preceding NOUN entities.
    private String succeedingNounEntity; // only for NER_NOUN. All succeeding NOUN entities.
    private List<String> synset;

    public AnnotatedEntity(String entity, EntityNameSpace namespace, List<String> synset) {
      this.entity = entity;
      this.namespace = namespace;
      this.synset = synset;
    }

    /**
     * since equality check is done using entity only, use the same for hashcode as well
     * @return
     */
    public int hashCode(){
      return entity.hashCode();
    }

    /**
     * if entity is same, then object is considered same
     * @param obj
     * @return
     */
    public boolean equals(Object obj){
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;

      // same entity means objects are same
      AnnotatedEntity objEntity = (AnnotatedEntity) obj;
      return entity.equalsIgnoreCase(objEntity.entity);
    }
  }

  /**
   * scan through all Entities and collect the one which match the scope (scope is always non-global for KGNER)
   * @param queryQuestion
   * @param entityExcludeSet
   * @param allPreds
   * @return
   */
  public static Set<AnnotatedEntity> getNEREntities(Question queryQuestion,
                                                    Set<String> entityExcludeSet,
                                                    Set<String> allPreds,
                                                    Pattern pattern) {
    LinkedHashSet<AnnotatedEntity> entities = new LinkedHashSet<>();

    if (CollectionUtils.isNotEmpty(queryQuestion.getNodesList())) {
      queryQuestion.getNodesList().stream()
          .filter(kgNode -> KGNER_TYPE_ENTITY.equalsIgnoreCase(kgNode.getType())) // exclude category
          .filter(kgNode -> StringUtils.isNotBlank(kgNode.getName()))
          .filter(kgNode ->  {
            if (pattern == null) {
              return true; // keep all, no regex pattern
            }
            // return true if none of the parent names match the regex
            return kgNode.getParentsList().stream()
              .map(KGNode::getName)
              .noneMatch(parentName -> pattern.matcher(parentName).matches());
          })
          .forEach(kgNode -> {
            String entityName = kgNode.getName();
            String rootEntity = StringUtils.normalizeSpace(entityName.toLowerCase());
            AnnotatedEntity item = new AnnotatedEntity(
                rootEntity,
                EntityNameSpace.NER,
                kgNode.getSynonymsList().stream()
                .map(token -> StringUtils.normalizeSpace(token.toLowerCase()))
                .filter(token -> !token.equalsIgnoreCase(rootEntity))
                .collect(Collectors.toList())
            );
            //O(1) operation time
            //Only include white listed entities and which has not been identified as PRED
            if(!entityExcludeSet.contains(item.getEntity().toLowerCase()) && !allPreds.contains(item.getEntity())) {
                entities.add(item);
            }
          });
    }
    return entities;
  }

  /**
   * converts to set of root entities having name only
   * @param entities
   * @return
   */
  // only used for logging
  public static String toStringRootEntitiesOnly(Set<AnnotatedEntity> entities) {
    LinkedHashSet<String> filtered = new LinkedHashSet<>();
    entities.forEach(entity -> filtered.add(entity.getEntity()));
    return String.join(", ", filtered);
  }

  /**
   * converts to set of root entities having name only for given namespace
   * @param entities
   * @return
   */
  public static Set<String> toRootEntitiesOnly(Set<AnnotatedEntity> entities, EntityNameSpace namespace) {
    LinkedHashSet<String> filtered = new LinkedHashSet<>();
    entities.stream().filter(item -> item.getNamespace() == namespace)
      .forEach(item -> filtered.add(item.getEntity()));
    return  filtered;
  }

  /**
   * find all nouns which is present after the predicate. Compound noun will be single noun.
   */
  private static Set<String> findAllNouns(QueryConfig queryConfig, Question queryQuestion,
                                          Set<String> predicateWordsToIgnore) {
    String tmp = queryConfig.getInvalidTokensInEntity();
    Set<String> invalidTokens =  new HashSet<>();
    if (StringUtils.isNotBlank(tmp)) {
      invalidTokens.addAll(
        Arrays.stream(tmp.split(","))
        .map(StringUtils::normalizeSpace)
        .collect(Collectors.toList())
      );
    }

    LinkedHashSet<String> allNouns = new LinkedHashSet<>();

    Sentence sentence = queryQuestion.getSentence();
    // now time to discover nouns
    // does our detected potential compound noun consist only of JJ* type of words so far?
    boolean nnDetectedOnlyJJ = false;
    List<String> nnDetected = new ArrayList<>();
    //detect type of word from WA model response
    for (TheWord word: sentence.getWordsList()) {
      // JJ may preceed NN in case of compound noun. In this case we want JJ
      // and NN together detected as Noun
      if ((isCompound(word) || isPotentialAdjectiveCompound(word)) &&
          !invalidTokens.contains(StringUtils.trim(word.getWord()))) {
        if (!predicateWordsToIgnore.contains(word.getWord())) {
          nnDetected.add(StringUtils.trim(word.getWord()));
          nnDetectedOnlyJJ = !isCompound(word) && isPotentialAdjectiveCompound(word);
        }
      } else {
        // Verb is found. Detect noun.
        if (word.getType().name().startsWith("NN")) {
          if ((!invalidTokens.contains(StringUtils.trim(word.getWord()))) && (!predicateWordsToIgnore.contains(word.getWord()))) {
            nnDetected.add(StringUtils.trim(word.getWord()));
          }
        } else {
          if (!nnDetected.isEmpty() && !nnDetectedOnlyJJ) {
            allNouns.add(StringUtils.normalizeSpace(String.join(" ", nnDetected)));
          }
          nnDetectedOnlyJJ = false;
          nnDetected.clear();
        }
      }
      if(allNouns.size() >= MAX_NOUN_SIZE) {
        break;
      }
    }

    // boundary case
    if (!nnDetected.isEmpty()) {
      allNouns.add(StringUtils.normalizeSpace(String.join(" ", nnDetected)));
    } else if (allNouns.isEmpty()){
      // in case we have no entity, last JJ will qualify as entity
      String lastJJ = "";
      for (TheWord word: sentence.getWordsList()) {
        if (word.getType().name().startsWith("JJ") && !invalidTokens.contains(StringUtils.trim(word.getWord()))) {
          lastJJ = StringUtils.trim(word.getWord());
        }
      }
      if (StringUtils.isNotBlank(lastJJ))
        allNouns.add(StringUtils.normalizeSpace(lastJJ));
    }
    // detected all nouns
    return allNouns;
  }

  private static boolean isPotentialAdjectiveCompound(TheWord word) {
    WordTag type = word.getType();
    return type.equals(WordTag.JJ) || type.equals(WordTag.JJR) || type.equals(WordTag.JJS);
  }

  private static boolean isCompound(TheWord word) {
    long compoundCount = word.getRelList().stream().map(WordRel::getType)
      .filter(subType -> subType == WordRelType.compound || subType == WordRelType.amod)
      .count();
    return (compoundCount > 0);
  }

  /**
   * Scan through WordAnnotation(WA) output in queryQuestion. Those word present in NER, mark them POS=NN
   * @param queryQuestion
   * @param nerEntities
   * @return
   */
  private static Question modifyPOSUsingNER(Question queryQuestion, Set<AnnotatedEntity> nerEntities) {
    try {
      if (!queryQuestion.hasSentence())
        return queryQuestion;
      // build set of all tokens present in NER
      Set<String> tokensInNER = new HashSet<>();
      for (AnnotatedEntity entity: nerEntities) {
        if (entity != null) {
          tokensInNER.addAll(Arrays.stream(entity.getEntity().toLowerCase().split(" "))
              .map(StringUtils::trim)
              .filter(item -> item.length() > 0)
              .collect(Collectors.toList()));
        }
      }
      // construct a new Builder for queryQuestion. Those word present in tokensInNER will be marked POS=NN
      Question.Builder builder = Question.newBuilder(queryQuestion);
      for (TheWord.Builder wordBuilder: builder.getSentenceBuilder().getWordsBuilderList()) {
        if (tokensInNER.contains(StringUtils.trim(wordBuilder.getWord()))) {
          wordBuilder.setType(WordTag.NN);
        }
      }
      return builder.build();
    } catch (Exception e) {
      return queryQuestion;
    }
  }

  public static Set<AnnotatedEntity> getNounEntities(QueryConfig queryConfig, Question queryQuestion,
                                                     Set<String> predicateWordsToIgnore,
                                                     Set<AnnotatedEntity> nerEntities,
                                                     Set<String> entityExcludeSet,
                                                     Set<String> allPreds) {
    LinkedHashSet<AnnotatedEntity> finalEntities = new LinkedHashSet<>();
    try {
      // get sentence type
      Question newQuestion = modifyPOSUsingNER(queryQuestion, nerEntities);
      if (newQuestion != null && newQuestion.hasSentence()) {
        // collect all noun entities as phrase
        Set<String> nounEntities = findAllNouns(queryConfig, queryQuestion, predicateWordsToIgnore);
        // scan all discovered noun entities phrase and build AnnotatedEntity
        for (String entity: nounEntities) {
          entity = StringUtils.trim(entity.toLowerCase());
          //O(1) operation time
          //Only include Nouns which are white listed entities and which has not been identified as PRED
          if(!entityExcludeSet.contains(entity.toLowerCase()) && !allPreds.contains(entity)) {
            finalEntities.add(new AnnotatedEntity(entity, EntityNameSpace.NOUN, new ArrayList<>()));
          }
        }
      }
    } catch (Exception e) {
      log.error("Failed to NER+Noun entities for reason:", e);
    }
    return finalEntities;
  }

  /**
   *
   * @param nerEntities
   * @param nounEntities
   * @return
   */
  public static Set<AnnotatedEntity> getNERPlusNounEntities(QueryConfig queryConfig,
                                                            Set<AnnotatedEntity> nerEntities,
                                                            Set<AnnotatedEntity> nounEntities) {
    LinkedHashSet<AnnotatedEntity> allEntities = new LinkedHashSet<>();
    boolean expandUtteranceByNERSynset = queryConfig.isExpandUtteranceByNERSynset();
    try {
      //top level we have two scenarios: NER={} or NER={...}
      // case: NER={} i.e. NER is empty
      if (nerEntities.isEmpty()) {
        // all Noun Entities will be treated as NER_NOUN in this case.
        for (AnnotatedEntity nounEntity : nounEntities) {
          nounEntity.setNamespace(EntityNameSpace.NER_NOUN);
          allEntities.add(nounEntity);
        }
      }
      // case: NER={...}  i.e. NER is not empty
      else {
        // 1. initialize expanded Entity with NOUN Entities
        LinkedHashSet<AnnotatedEntity> expandedEntities = new LinkedHashSet<>(nounEntities);

        // 2. scan Expanded Entities. If it includes any NER Entity, annotate it with NER Entity data as well as set namespace=NER_NOUN
        for (AnnotatedEntity nounEntity : expandedEntities) {
          String nounEntityPhrase = " " + StringUtils.normalizeSpace(nounEntity.getEntity()) + " ";
          for (AnnotatedEntity nerEntity : nerEntities) {
            if (nounEntityPhrase.contains(" " + nerEntity.getEntity() + " ")) {
              nounEntity.setNerSubEntity(nerEntity.getEntity());
              nounEntity.setNamespace(EntityNameSpace.NER_NOUN);
              // build updated synset from NER synset by replace NER text in Noun Entity with synonym text.
              List<String> updatedSynset = new ArrayList<>();
              if (expandUtteranceByNERSynset) {
                for (String synonym : nerEntity.getSynset()) {
                  // replace NER text in NOUN Entity by synonym
                  String tmp = (" " + nounEntity.getEntity() + " ")
                    .replaceAll(" " + nerEntity.getEntity() + " ", " " + synonym + " ");
                  updatedSynset.add(StringUtils.normalizeSpace(tmp));
                }
              }
              nounEntity.setSynset(updatedSynset);
            }
          }
        }

        // 3. collect all NER entities which are not included (fully/partially) in expanded entities.
        LinkedHashSet<AnnotatedEntity> pureNEREntities = new LinkedHashSet<>();
        for (AnnotatedEntity nerEntity : nerEntities) {
          boolean foundInExpanded = false;
          for (AnnotatedEntity expandedEntity : expandedEntities) {
            if (expandedEntity.getNerSubEntity() != null && expandedEntity.getNerSubEntity().equalsIgnoreCase(nerEntity.getEntity())) {
              foundInExpanded = true;
              break;
            }
          }
          if (!foundInExpanded) {
            pureNEREntities.add(nerEntity);
          }
        }

        // 4. scan Expanded Entities and merge those with namespace=NOUN to next available entity with namespace=NER_NOUN
        LinkedHashSet<AnnotatedEntity> decoratedExpandedEntities = new LinkedHashSet<>();
        StringBuilder precedingNouns = new StringBuilder();
        AnnotatedEntity lastNerNounEntity = null;
        for (AnnotatedEntity expanded : expandedEntities) {
          if (expanded.getNamespace() == EntityNameSpace.NOUN) {
            precedingNouns.append(" ");
            precedingNouns.append(expanded.getEntity());
          } else {
            lastNerNounEntity = expanded;
            String tmp = StringUtils.normalizeSpace(precedingNouns.toString());
            if (StringUtils.isNotBlank(tmp)) {
              expanded.setPrecedingNounEntity(tmp);
            }
            decoratedExpandedEntities.add(expanded);
            precedingNouns.setLength(0);
          }
        }

        // boundary case
        if (lastNerNounEntity != null && StringUtils.isNotBlank(StringUtils.normalizeSpace(precedingNouns.toString()))) {
          lastNerNounEntity.setSucceedingNounEntity(StringUtils.normalizeSpace(precedingNouns.toString()));
        }

        // 5. prepend all NER entities to Expanded Entities which are not present in Expanded Entities
        allEntities.addAll(pureNEREntities);
        allEntities.addAll(decoratedExpandedEntities);
      }

    } catch (Exception e) {
      log.error("Failed to NER+Noun entities for reason:", e);
    }
    return allEntities;
  }

  /**
   * Compute entity utterances for PRED+ENTITY generation from VALID_EXPANDED_NER2_ENTITIES
   *   1. in-order combination of all NER_NOUN entities
   *   2. a single utterance of in-order combination of all NER_NOUN and NOUN
   * @param expandedNER2Entities
   * @return
   */
  public static Set<String> genEntityUtterancesForSPAN(Set<AnnotatedEntity> expandedNER2Entities) {
    LinkedHashSet<String> genEntityUtterances = new LinkedHashSet<>();
    try {
      List<String> prevEntityUtterances = new ArrayList<>();
      // initialize base set
      prevEntityUtterances.add("");
      for (AnnotatedEntity entity: expandedNER2Entities) {
        // 1. generate and hold current entity utterance.
        // 2. add them to final utterances list
        // 3. re-initialize base set.
        List<String> currEntityUtterance = new ArrayList<>();
        String precedingNounEntity = ""; // Noun entities preceding qualified entity
        String succeedingNounEntity = ""; // Noun entity succeeding qualified entity
        if (entity.getNamespace() == EntityNameSpace.NER_NOUN || entity.getNamespace() == EntityNameSpace.NER) {
          // construct phrase for preceding and succeeding Noun Entity
          if (StringUtils.isNotBlank(entity.getPrecedingNounEntity()))
            precedingNounEntity =  " " + entity.getPrecedingNounEntity() + " ";
          if (StringUtils.isNotBlank(entity.getSucceedingNounEntity()))
            succeedingNounEntity =  " " + entity.getSucceedingNounEntity() + " ";

          // run through each of previously existing utterances
          for (String prevUtterance : prevEntityUtterances) {
            // add core entity to utterance
            currEntityUtterance.add(prevUtterance + " " + entity.getEntity()); // add root entity to base utterance
            // if preceding/succeeding entity is present, add them along with core entity to utterance
            if (StringUtils.isNotBlank(precedingNounEntity) || StringUtils.isNotBlank(succeedingNounEntity))
              currEntityUtterance.add(prevUtterance + " " + precedingNounEntity + entity.getEntity() + succeedingNounEntity);

            // same for synset. First add synset to utterance. Then add along with preceding/succeeding noun in case they exist.
            List<String> synset = entity.getSynset();
            if(synset.size() > MAX_SYNSET_SIZE) {
              synset = entity.getSynset().subList(0, MAX_SYNSET_SIZE);
            }
            for (String synsetEntity : synset) {
              currEntityUtterance.add(prevUtterance + " " + synsetEntity); // adding synset to base utterance
              if (StringUtils.isNotBlank(precedingNounEntity) || StringUtils.isNotBlank(succeedingNounEntity))
                currEntityUtterance.add(prevUtterance + " " + precedingNounEntity + synsetEntity + succeedingNounEntity);
            }
          }
          // add currently generated entity utterances to final list.
          for (String item : currEntityUtterance) {
            genEntityUtterances.add(StringUtils.normalizeSpace(item));
          }
          // re-initialize base set
          prevEntityUtterances.clear();
          prevEntityUtterances.addAll(currEntityUtterance);
        }
      }
    } catch (Exception e) {
      log.error("Failed to generate Entity utterance for SPAN for reason:", e);
    }
    return genEntityUtterances.stream()
      .map(StringUtils::normalizeSpace)
      .collect(Collectors.toCollection(LinkedHashSet::new));
  }


  /**
   * Compute utterance of entities for ENTITY SPAN search from VALID_EXPANDED_NER2_ENTITIES
   *   1. in-order combination of all NER_NOUN entities
   *   2. a single utterance of in-order combination of all NER_NOUN and NOUN
   *   3. drop those which has single token
   * @param expandedNER2Entities
   * @param entityUtterancesForSPAN
   * @return
   */
  public static Set<String> genSPANEntitiesForSearch(QueryConfig queryConfig,
                                                     Set<AnnotatedEntity> expandedNER2Entities,
                                                     Set<String> entityUtterancesForSPAN) {
    LinkedHashSet<String> entityUtterances = new LinkedHashSet<>();
    int limit = queryConfig.getMaxPredEntityUtterances();
    try {
        for (String item : entityUtterancesForSPAN) {
          if (StringUtils.isNotBlank(item) && item.indexOf(" ") > 0) { // ensures that we have more than one token
            // make sure that we are not adding a multi-word synonym
            boolean isMultiWordSynonym = false;
            for (AnnotatedEntity annotatedEntity : expandedNER2Entities) {
              for (String synonym : annotatedEntity.getSynset()) {
                if (item.equals(synonym)) {
                  isMultiWordSynonym = true;
                  break;
                }
              }
              if (isMultiWordSynonym) {
                break;
              }
            }

            if (!isMultiWordSynonym) {
              entityUtterances.add(item);
            }
          }
          if(entityUtterances.size() >= limit){
            break;
          }
        }
    } catch (Exception e) {
      log.error("Failed to generate Entity utterance entity SPAN for reason:", e);
    }
    return entityUtterances;
  }

  /**
   * Entities in VALID Expanded Entities (NER_NOUN, NER)
   *  and {ones present in NER2 but not present in expandedNER2Entities in exact same form}
   * 1. all NER_NOUN entity
   * 2. all NER entity
   * @param expandedNER2Entities
   * @return
   */
  public static Set<String> genRegularEntitiesForSearch(QueryConfig queryConfig,
                                                        Set<AnnotatedEntity> expandedNER2Entities,
                                                        Set<AnnotatedEntity> filteredNEREntities) {
    LinkedHashSet<String> entityUtterances = new LinkedHashSet<>();
    try {
      int limit = queryConfig.getMaxPredEntityUtterances();
        // add from VALID Expanded Entities (NER_NOUN, NER)
        for (AnnotatedEntity item: expandedNER2Entities) {
          if (item.getNamespace() == EntityNameSpace.NER_NOUN || item.getNamespace() == EntityNameSpace.NER) {
            entityUtterances.add(item.getEntity());
            entityUtterances.addAll(item.getSynset());
          }
        }
        // add ones present in NER2 but not present in expandedNER2Entities in exact same form
      if(entityUtterances.size() < limit) {
        for (AnnotatedEntity nerEntity :filteredNEREntities) {
          if (expandedNER2Entities.stream().noneMatch(item -> item.getEntity().equalsIgnoreCase(nerEntity.getEntity()))) {
            entityUtterances.add(nerEntity.getEntity());
            entityUtterances.addAll(nerEntity.getSynset());
          }
        }
      }
    } catch (Exception e) {
      log.error("Failed to generate Entity utterance for reason: ", e);
    }
    return entityUtterances;
  }

  /**
   * Generate user UTTERANCES by expanding PRED and ENTITY.
   * @param allPreds
   * @param entityUtterancesForSPAN
   * @return
   */
  public static Set<String> genPredEntityUtterance(QueryConfig queryConfig,
                                                   Set<String> allPreds,
                                                   Set<String> entityUtterancesForSPAN) {
    LinkedHashSet<String> userUtterances = new LinkedHashSet<>();
    int limit = queryConfig.getMaxPredEntityUtterances();
    try {
      // if no PRED OR no ENTITY, then we are done.
      if (allPreds.isEmpty() || entityUtterancesForSPAN.isEmpty())
        return userUtterances;

      // get symmetric synset
      Map<String, List<String>> symmetricSynset = queryConfig.getSynset().getSymmetricSynsetMap();

      Map<String, List<String>> predMap = new HashMap<>();
      for (String pred: allPreds) {
        if (symmetricSynset != null && symmetricSynset.containsKey(pred))
          predMap.put(pred, symmetricSynset.get(pred));
        else
          predMap.put(pred, new ArrayList<>());
      }

      // we have PRED and ENTITY map. Generate user utternaces now.
      // case-1: PRED is not empty as well as ENTITY is not empty
      LinkedHashSet<String> tmpUtterances = new LinkedHashSet<>();
      for (Map.Entry<String, List<String>> predEntry: predMap.entrySet()) {
        tmpUtterances.add(predEntry.getKey());
        tmpUtterances.addAll(predEntry.getValue());
      }

      for (String entity : entityUtterancesForSPAN) {
        LinkedHashSet<String> newUtterances = new LinkedHashSet<>();
        // TODO: generate utterance for synset
        for (String existingUtterance: tmpUtterances) {
          newUtterances.add(existingUtterance + " " + entity);
        }
        userUtterances.addAll(newUtterances);
        if(userUtterances.size() >= limit) {
          break;
        }
      }
      userUtterances = new LinkedHashSet<>(removeDuplicateTokens(userUtterances));
    } catch (Exception e) {
      log.error("generating user utterances by PRED,ENTITY expansion failed for reason: ", e);
    }
    return userUtterances;
  }

  /**
   * remove deuplicate tokens from utterances
   * @param utterances
   * @return
   */
  private static Set<String> removeDuplicateTokens(Set<String> utterances) {
    if (utterances.isEmpty())
      return utterances;
    LinkedHashSet<String> finalUtterances = new LinkedHashSet<>();
    for (String text : utterances) {
      if (StringUtils.isBlank(text))
        continue;

      String[] tokens = text.split(" ");
      if (tokens.length == 0)
        continue;

      StringBuilder sb = new StringBuilder();
      Set<String> uniqueTokens = new HashSet<>();
      for (String token : tokens) {
        token = StringUtils.trim(token);
        if (!uniqueTokens.contains(token)) {
          uniqueTokens.add(token);
          sb.append(" ");
          sb.append(token);
        }
      }
      finalUtterances.add(StringUtils.normalizeSpace(sb.toString()));
    }
    return finalUtterances;
  }

  /**
   * Do we have seen form of "has". If yes, return that else null.
   * @param queryQuestion
   * @return
   */
  private static String getSeenFormOfHas(Question queryQuestion) {
    for (TheWord word: queryQuestion.getSentence().getWordsList()) {
      String token = StringUtils.trim(word.getWord());
      if (token.equalsIgnoreCase("have") || token.equalsIgnoreCase("having")
        || token.equalsIgnoreCase("has")) {
        return token;
      }
    }
    return null;
  }

  /**
   * Looks for verb in queryQuestion. If the next word right after is NN, return word else null
   * @param verb
   * @param queryQuestion
   * @return
   */
  private static String firstNounAfterVerb(String verb, Question queryQuestion) {
    String nounAfterVerb = "";
    boolean verbFound = false;
    // now time to dicover nouns
    String nnDetected = "";
    for (TheWord word: queryQuestion.getSentence().getWordsList()) {
      if (verb.equalsIgnoreCase(StringUtils.trim(word.getWord()))) {
        verbFound = true;
        continue;
      }
      if (!verbFound)
        continue;

      // JJ may preceed NN in case of compound noun. In this case we want JJ
      // and NN together detected as Noun
      if (isCompound(word)) {
        continue;
      }
      // Verb is found. Detect noun.
      if (word.getType().name().startsWith("NN")) {
        nnDetected = StringUtils.trim(word.getWord());
        continue;
      }

      if (StringUtils.isNotBlank(nnDetected)) {
        nounAfterVerb = nnDetected;
      }
      break;
    }
    return StringUtils.isBlank(nounAfterVerb) ? null : StringUtils.trim(nounAfterVerb);
  }

  private static TheWord discoverWord(Question queryQuestion, String token) {
    Sentence sentence = queryQuestion.getSentence();
    TheWord wordFound = null;
    for (TheWord word: sentence.getWordsList()) {
      if (token.equalsIgnoreCase(word.getWord())) {
        wordFound = word;
        break;
      }
    }
    return wordFound;
  }

  @Data
  public static class DiscoveredPredicates {
    private Set<String> allPreds; // predicates to use for PREDICATE SPAN
    private Set<String> waPreds; // predicates not used for queries but that should not be considered as nouns
  }

  // Use predicate from model if available.
  // Calculate WA predicates to be used as noun filters, use WA predicates as predicates if pred-obj results were empty.
  public static DiscoveredPredicates getAllPreds(Question queryQuestion) {
    LinkedHashSet<String> predObjPreds = new LinkedHashSet<>();
    for (Quad quad : queryQuestion.getQuadsList()) {
      if (quad.hasPredicate()) {
        String predicate = quad.getPredicate().getWords();
        if (StringUtils.isNotBlank(predicate) && !"None".equals(predicate)) {
          predObjPreds.add(predicate);
        }
      }
    }

    Set<String> waPreds = getAllWAPreds(queryQuestion);

    DiscoveredPredicates discoveredPredicates = new DiscoveredPredicates();
    if (!predObjPreds.isEmpty()) {
      discoveredPredicates.setAllPreds(predObjPreds);
      discoveredPredicates.setWaPreds(waPreds);
    } else {
      discoveredPredicates.setAllPreds(waPreds);
      discoveredPredicates.setWaPreds(new LinkedHashSet<>());
    }

    return discoveredPredicates;
  }
  /**
   * We will extract predicate from WordAnnotation(WA) model using following rules
   * 1) if WA returns only one verb use it as predicate to expand
   * 2) if returns WA more than 1, use 2nd one as predicate to expand
   * 3) if WA returns one verb and one noun, if noun is different than entity then use noun as predicate for expansion
   * 4) if WA returns no VERB, look into noun as candidate predicate. Those not present in NER2 will qualify as predicate
   * @param queryQuestion
   * @return
   */
  private static Set<String> getAllWAPreds(Question queryQuestion) {
    LinkedHashSet<String> allPreds = new LinkedHashSet<>();
    try {
      if (!queryQuestion.hasSentence())
        return allPreds;
      Sentence sentence = queryQuestion.getSentence();
      List<String> verbs = new ArrayList<>();
      for (TheWord word: sentence.getWordsList()) {
        if (word.getType().name().startsWith("VB")) {
          verbs.add(word.getWord());
        }
      }

      // #3: if WA returns one verb and one noun, if noun is not included in entity then use noun as predicate for expansion
      // improvement note:
      if (verbs.size() >= 1) {
        String seenFormOfHas = getSeenFormOfHas(queryQuestion);
        if (seenFormOfHas != null) {
          // if we have noun after has/have/..., get that and consider it as Verb
          String nounAsPredicate = firstNounAfterVerb(seenFormOfHas, queryQuestion);
          if (nounAsPredicate != null) {
            allPreds.add(nounAsPredicate);
          } else {
            allPreds.add(seenFormOfHas);
          }
          return allPreds;
        }
      }

      // #2a: 1. [verb1 "to" verb2] --> PRED=verb2
      //      2. [verb1 noun "to" verb2] --> PRED=verb2
      //      3. [verb1 ... verb2 verb3] --> PRED=verb3 | verb2={"want","be","was","were","has","have","had"}
      if (verbs.size() > 1 ) {
        // rule #2a:1  [verb1 "to" verb2] --> PRED=verb2
        TheWord currMinusOne = null;
        TheWord currMinusTwo = null;
        for (TheWord curr : sentence.getWordsList()) {
          if (currMinusOne == null) {
            currMinusOne = curr;
            continue;
          } else if (currMinusTwo == null) {
            currMinusTwo = currMinusOne;
            currMinusOne = curr;
            continue;
          }
          if (curr.getType().name().startsWith("VB")) {
            if (StringUtils.trim(currMinusOne.getWord()).equalsIgnoreCase("to")
              && currMinusTwo.getType().name().startsWith("VB")) {
              // if current verb=="be", skip it and focus on next verb
              if (StringUtils.trim(curr.getWord()).equalsIgnoreCase("be"))
                continue;
              // found PRED
              allPreds.add(StringUtils.trim(curr.getWord()));
              log.info("Predicate was discovered by rule#2a:1");
              return allPreds;
            }
          } else {
            currMinusTwo = currMinusOne;
            currMinusOne = curr;
          }
        }

        // rule #2a:2  [verb1 noun "to" verb2] --> PRED=verb2
        currMinusOne = null;
        currMinusTwo = null;
        TheWord currMinusThree = null;
        for (TheWord curr : sentence.getWordsList()) {
          if (currMinusOne == null) {
            currMinusOne = curr;
            continue;
          } else if (currMinusTwo == null) {
            currMinusTwo = currMinusOne;
            currMinusOne = curr;
            continue;
          } else if (currMinusThree == null) {
            currMinusThree = currMinusTwo;
            currMinusTwo = currMinusOne;
            currMinusOne = curr;
            continue;
          }
          if (curr.getType().name().startsWith("VB")) {
            if (StringUtils.trim(currMinusOne.getWord()).equalsIgnoreCase("to")
                && currMinusTwo.getType().name().startsWith("NN")
                && currMinusThree.getType().name().startsWith("VB")) {
              // found PRED
              allPreds.add(StringUtils.trim(curr.getWord()));
              log.info("Predicate was discovered by rule#2a:2");
              return allPreds;
            }
          } else {
            currMinusThree = currMinusTwo;
            currMinusTwo = currMinusOne;
            currMinusOne = curr;
          }
        }

        // rule #2a:3  [verb1 ... verb2 verb3] --> PRED=verb3 | verb2={"want","be","was","were","has","have","had"}
        TheWord verb1 = null;
        TheWord verb2 = null;
        TheWord verb3;
        List<String> verb2Candidates = Arrays.asList("want","be","was","were","has","have","had");
        for (TheWord curr : sentence.getWordsList()) {
          if (curr.getType().name().startsWith("VB")) {
            if (verb1 == null) {
              verb1 = curr;
            } else if (verb2 == null) {
              // current word qualifies as verb2 only if curr={"be","was","were","has","have","had"}
              if (verb2Candidates.contains(StringUtils.trim(curr.getWord()))) {
                verb2 = curr;
              } else {
                verb1 = curr;
              }
            } else {
              verb3 = curr;
              // we have found verb1, verb2 and verb3. Apply PRED=verb3
              allPreds.add(StringUtils.trim(verb3.getWord()));
              log.info("Predicate was discovered by rule#2a:3");
              return allPreds;
            }
          }
        }
      }

      // #2b: if WA returns more than 1, use 2nd one as predicate only if it is not compund otherwise use first verb
      if (verbs.size() > 1) {
        TheWord candidateVerb = discoverWord(queryQuestion, verbs.get(1));
        if (candidateVerb != null && !isCompound(candidateVerb))  {
          allPreds.add(StringUtils.trim(verbs.get(1)));
        } else {
          allPreds.add(StringUtils.trim(verbs.get(0)));
        }
        log.info("Predicate was discovered by rule#2");
        return allPreds;
      }

      // #1: if WA returns only one verb use it as predicate to expand
      if (verbs.size() == 1) {
        allPreds.add(StringUtils.trim(verbs.get(0)));
        log.info("Predicate was discovered by rule#1");
        return allPreds;
      }

      // if NO VERB, then noun preceeding "to" will be verb.
      // e.g. in case "access to windows group", the word "access" is verb
      // if (verbs.size() == 0)
      // verb.size() == 0 is always true at this point since verbs.size() < 1 from the above if statements
      TheWord prevWord = null;
      for (TheWord currWord :sentence.getWordsList()) {
        if (prevWord != null && prevWord.getType().name().startsWith("NN")
            && StringUtils.trim(currWord.getWord()).equalsIgnoreCase("to")) {
          allPreds.add(StringUtils.trim(prevWord.getWord()));
          break;
        }
        prevWord = currWord;
      }
      log.info("Predicate was discovered by rule#4");
      return allPreds;
    } catch (Exception e) {
      log.error("Extracting predicate failed for reason: ", e);
    }
    return allPreds;
  }

  /**
   * extract collection of superIntents from PredicateObject model output
   * @param queryQuestion
   * @param allEntities
   * @return
   */
  public static Set<String> getSuperIntents(Question queryQuestion,
                                            Set<AnnotatedEntity> allEntities) {
    Set<String> superIntents = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    try {
      // pick up only those super intents which include entity
      allEntities.stream()
        .map(AnnotatedEntity::getEntity)
        .forEach(entity ->
          queryQuestion.getQuadsList().forEach(quad -> {
            if (quad.hasSuperIntent() && StringUtils.isNotBlank(quad.getSuperIntent().getWords())
              && quad.getSuperIntent().getWords().split(" ").length >= 2) {
              if (quad.getSuperIntent().getWords().contains(entity)) {
                superIntents.add(quad.getSuperIntent().getWords());
              }
            }
          }));
    } catch (Exception e) {
      log.error("Building superIntents set failed for reason: ", e);
    }
    return superIntents;
  }

  /**
   * Remove black listed entities
   * @param allEntities
   * @return
   */
  public static Set<AnnotatedEntity> filterEntityByExcludeList(Set<String> entityExcludeSet,
                                                               Set<AnnotatedEntity> allEntities) {
    try {
      LinkedHashSet<AnnotatedEntity> filtered = new LinkedHashSet<>();
      allEntities.forEach(item -> {
        if(!entityExcludeSet.contains(item.getEntity().toLowerCase()))
          filtered.add(item);
      });
      return filtered;
    } catch(Exception e) {
      return allEntities;
    }
  }
}
