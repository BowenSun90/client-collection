package com.alex.space.elastic.entity;

public class EsResult {

  private int took;
  private boolean timed_out;
  private EsShards _shards;

  private EsHitsResult hits;

  public int getTook() {
    return took;
  }

  public void setTook(int took) {
    this.took = took;
  }

  public boolean isTimed_out() {
    return timed_out;
  }

  public void setTimed_out(boolean timed_out) {
    this.timed_out = timed_out;
  }

  public EsShards get_shards() {
    return _shards;
  }

  public void set_shards(EsShards _shards) {
    this._shards = _shards;
  }

  public EsHitsResult getHits() {
    return hits;
  }

  public void setHits(EsHitsResult hits) {
    this.hits = hits;
  }


}
