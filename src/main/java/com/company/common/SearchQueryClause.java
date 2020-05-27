package com.company.common;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SearchQueryClause {

  protected ClauseType type;
  protected String fieldName;
  protected String text;

  public enum ClauseType {
    ALL,
    MATCH,
    MATCH_TERM,
    MATCH_PHRASE,
    MULTI_MATCH
  }
}
