package com.company.common;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Builder
public class SearchResult {

  @Getter @Setter
  private String id;
  @Getter @Setter
  private String indexId;
  @Getter @Setter
  private String[] matchedQueries;
  @Getter @Setter
  private String contentId;
  @Getter @Setter
  private String type;
  @Getter @Setter
  private float score;
  @Getter
  private ResultPosition position;
  @Getter @Setter
  private String title;
  @Getter
  private String titleDesc;
  @Getter
  private String sourceAddress;
  @Getter @Setter
  private String sourceAsString;
  @Getter
  private String rawDataURI;
  @Getter
  private String documentTitle;
  @Getter
  private List<MatchedKeywords> matches;
  @Getter
  private List<MatchStat> matchStats;

  @Builder
  public static class ResultPosition {
    @Getter
    private long position;
    @Getter
    private long queryHitCount;
  }

  @Builder
  public static class MatchedKeywords {
    @Getter
    List<String> keywords;
    @Getter
    String type;
    @Getter
    String field;
    @Getter
    float score;
  }

  @Builder
  public static class MatchStat {
    @Getter
    String type;
    @Getter
    String field;
    @Getter
    int totalCount;
    @Getter
    int deDupCount;
    @Getter
    float totalScore;
  }
}
