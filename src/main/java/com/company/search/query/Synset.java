package com.company.search.query;

import com.aisera.common.io.ObjectStoreFactory;
import com.aisera.common.utils.ObjectStoreUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

@Slf4j
public class Synset {
  private static final Map<String, Synset> synsetsMap = new HashMap<>();
  private static final ObjectStoreFactory.ObjectStore s3Client = ObjectStoreUtil.getS3Client();
  @Getter private Map<String, List<String>> symmetricSynsetMap;
  @Getter private Map<String, String> utterancePreprocessMap;
  private static final String synsetFileName = "basic_synsets.csv";

  private static String getSynsetKeyForTenant(@SuppressWarnings("unused") String tenantId) {
    // TODO: call a function below to get different files per tenant
    // return "ConversationData/basic_synsets.csv";
    //return null; // fallback to local default
    return synsetFileName;
  }

  private static String getUtteranceMappingKeyForTenant(@SuppressWarnings("unused") String tenantId) {
    // TODO: call a function below to get different files per tenant
    // return "ConversationData/utterance_preprocess.csv";
    return null; // fallback to local default
  }

  /**
   * Load synset for predicate. Build AsymmetricMap and SymmetricMap for the words present in each record.
   * Returns Synset object of both maps.
   *
   * @param tenantId
   * @return
   */
  static Synset getSynset(String tenantId) {
    // we have it pre-built
    if (synsetsMap.containsKey(tenantId)) {
      return synsetsMap.get(tenantId);
    }
    synchronized (Synset.class) {
      Synset synsetCache = new Synset();
      // First we load predicate/entity synset and build cache
      InputStream csvStream = Synset.class.getResourceAsStream(synsetFileName); // local default

      String synsetKeyForTenant = getSynsetKeyForTenant(tenantId);
      if (synsetKeyForTenant != null) {
        log.info("Loading syn-set file from S3 for predicate expansion: {}", synsetKeyForTenant);
        try {
          csvStream = s3Client.getAsStream(tenantId, synsetKeyForTenant);
          log.info("For predicate syn-set, got S3 file from: {}", synsetKeyForTenant);
        } catch (Exception e) {
          log.error("Failed to get syn-set for tenant: {}, key: {}", tenantId, synsetKeyForTenant, e);
        }
      }

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(csvStream))) {
        Map<String, List<String>> symmetricSynset = new HashMap<>();
        // scan through each line
        String line;
        while ((line = reader.readLine()) != null) {
          // skip comments
          if (line.charAt(0) == '#')
            continue;
          // holds list of all tokens
          line = StringUtils.normalizeSpace(line.toLowerCase());
          List<String> orderedSynset = new ArrayList<>();
          // split record in two part
          if (StringUtils.isNotBlank(line)) {
            String[] data = line.split(",", 2);
            // process right hand side of record
            if (data.length == 2) {
              // extract synset words
              if (StringUtils.isNotBlank(data[1])) {
                // split right side by comma separator. Each of them are valid tokens for synset
                String[] synset = data[1].replaceAll("\"", "").split(",");
                // if we have valid data on right hand side, build ordered list of synset
                if (synset.length >= 1) {
                  orderedSynset.add(StringUtils.trim(data[0]));
                  for (String word : synset) {
                    if (!orderedSynset.contains(StringUtils.trim(word)))
                      orderedSynset.add(StringUtils.trim(word));
                  }
                  // use the ordered list to build asymmetric, symmetric synset
                  buildSynset(orderedSynset, symmetricSynset);
                }
              }
            }
          }
        }
        synsetCache.symmetricSynsetMap = symmetricSynset;
      } catch (Exception e) {
        log.error("Failed to load synset for reason: ", e);
      }

      // load utterance preprocess map into cache
      InputStream utteranceCsvStream = Synset.class.getResourceAsStream("/utterance_preprocess.csv"); // local default
      String utteranceKeyForTenant = getUtteranceMappingKeyForTenant(tenantId);
      if (utteranceKeyForTenant != null) {
        try {
          utteranceCsvStream = s3Client.getAsStream(utteranceKeyForTenant);
          log.info("For utterance preprocess, got S3 file from: {}", utteranceKeyForTenant);
        } catch (Exception e) {
          log.error("Failed to get utterance preprocess map for tenant: {}, key: {}, for reason: {}", tenantId, utteranceKeyForTenant, e);
        }
      }

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(utteranceCsvStream))) {
        Map<String, String> utterancePreprocessMap = new HashMap<>();
        // scan through each line
        String line;
        while ((line = reader.readLine()) != null) {
          // skip comments
          if (line.charAt(0) == '#')
            continue;
          line = StringUtils.normalizeSpace(line.toLowerCase());
          // split record in two part
          if (StringUtils.isNotBlank(line)) {
            String[] data = line.split(",", 2);
            if (data.length == 2) {
              utterancePreprocessMap.put(StringUtils.trim(data[0]), StringUtils.trim(data[1]));
            }
          }
        }
        synsetCache.utterancePreprocessMap = utterancePreprocessMap;
      } catch (Exception e) {
        log.error("Failed to load utterance preprocess map.", e);
      }
      synsetsMap.put(tenantId, synsetCache);
    }
    printSynsetCache(synsetsMap.get(tenantId));
    return synsetsMap.get(tenantId);
  }

  private static void printSynsetCache(Synset synset) {
    if (synset.utterancePreprocessMap != null && !synset.utterancePreprocessMap.isEmpty()) {
      log.debug("printing utterance map:");
      for (Map.Entry<String, String> entry : synset.utterancePreprocessMap.entrySet()) {
        log.debug("  {} --> {}", entry.getKey(), entry.getValue());
      }
    }
    if (synset.symmetricSynsetMap != null && !synset.symmetricSynsetMap.isEmpty()) {
      log.debug("printing predicate symmetric map:");
      for (Map.Entry<String, List<String>> entry : synset.symmetricSynsetMap.entrySet()) {
        log.debug("  {} --> {}", entry.getKey(), String.join(", ", entry.getValue()));
      }
    }
  }

  /**
   * using ordered list of synset, build symmetric and asymmetric maps of synset
   *
   * @param orderedSynset
   * @param symmetricSynset
   */
  private static void buildSynset(List<String> orderedSynset, Map<String, List<String>> symmetricSynset) {
    for (int idx = 0; idx < orderedSynset.size(); idx++) {
      List<String> data = new ArrayList<>(orderedSynset.subList(0, idx));
      data.addAll(orderedSynset.subList(idx + 1, orderedSynset.size()));
      // construct symmetric synset. If Already entry exists then take union
      if (symmetricSynset.get(orderedSynset.get(idx)) == null) {
        symmetricSynset.put(orderedSynset.get(idx), data);
      } else {
        // take union of already existing and current one
        Set<String> synset = new HashSet<>();
        synset.addAll(symmetricSynset.get(orderedSynset.get(idx)));
        synset.addAll(data);
        symmetricSynset.put(orderedSynset.get(idx), new ArrayList<>(synset));
      }
    }
  }
}
