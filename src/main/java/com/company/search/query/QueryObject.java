package com.company.search.query;

import com.aisera.common.Question;
import com.aisera.common.SourceChannelType;
import com.aisera.common.config.SystemConfig;
import com.aisera.common.utils.TimerUtils;
import com.aisera.modelSDK.IntentClassificationOutput;
import com.aisera.nlp.INLPService;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Builder(toBuilder = true)
@Data
@ToString
public class QueryObject {
  String tenantId;
  Long botId;
  String userId;
  String userEmail; // used for fetching user profile
  List<SourceChannelType> searchItemTypes;
  List<SystemConfig.SystemConfigMapper.SearchType> searchOrder;
  com.aisera.common.SearchType searchType;
  String queryStr;
  boolean asHtml; // TODO: this is violating Search services responsibility to fetch results, not format them
  com.aisera.search.query.QueryConfig queryConfig;
  @Builder.Default
  List<String> conversationContentIds = new ArrayList<>();

  // used for passing cached model results
  IntentClassificationOutput icmOutput;
  private Question question;
  private final AtomicBoolean isNlpCalled = new AtomicBoolean();
  boolean debugMode;

  // used for hyper-parameter optimisation
  String searchConfigOverride;

  public Question getQuestion() {
    if (isNlpCalled.compareAndSet(false, true)) {
      TimerUtils tu = new TimerUtils(this.getClass().getCanonicalName(), "INLPService.process");
      INLPService extractor = INLPService.getSemanticExtractor();
      //get Size limited Search String to limit nouns, entities and predicates, also reduce load on models
      question = extractor.process(tenantId, getQuestionForNlpRequest());
      tu.stop();
    }

    return question;
  }

  private Question getQuestionForNlpRequest() {
    String searchStr = StringUtils.isNotEmpty(queryStr) ? queryStr : "";
    return Question.newBuilder().setText(searchStr).build();
  }

  public int getMaxResults() {
    return queryConfig.getMaxSearchResults();
  }
}
