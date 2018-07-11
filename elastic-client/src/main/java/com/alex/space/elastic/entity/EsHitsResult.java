package com.alex.space.elastic.entity;

import java.util.List;

public class EsHitsResult {

  private long total;
  private double max_score;
  private List<EsHitsRecord> hits;

  public long getTotal() {
    return total;
  }

  public void setTotal(long total) {
    this.total = total;
  }

  public double getMax_score() {
    return max_score;
  }

  public void setMax_score(double max_score) {
    this.max_score = max_score;
  }

  public List<EsHitsRecord> getHits() {
    return hits;
  }

  public void setHits(List<EsHitsRecord> hits) {
    this.hits = hits;
  }

}
