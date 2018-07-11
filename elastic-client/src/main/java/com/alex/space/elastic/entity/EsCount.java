package com.alex.space.elastic.entity;

public class EsCount {

  private long count;
  private EsShards _shards;

  public EsShards get_shards() {
    return _shards;
  }

  public void set_shards(EsShards _shards) {
    this._shards = _shards;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

}
