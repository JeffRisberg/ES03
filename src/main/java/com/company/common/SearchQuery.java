package com.company.common;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Getter
@Builder
@Slf4j
public class SearchQuery {

  @Setter
  @Singular
  protected List<String> indexes;

  @Singular
  protected List<SearchQueryClause> clauses;

  @Setter
  private Sentence sentence;

  private Map<String, List<String>> synonymsMap;

  public boolean hasTokens() {
    return ((this.sentence != null) && (this.sentence.getWordCount() > 0));
  }
}
