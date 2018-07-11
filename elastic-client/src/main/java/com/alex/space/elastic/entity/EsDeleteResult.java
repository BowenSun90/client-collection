package com.alex.space.elastic.entity;

public class EsDeleteResult {

  private boolean found;
  private String _index;
  private String _type;
  private String _id;
  private String _version;

  private EsShards _shards;

  public EsShards get_shards() {
    return _shards;
  }

  public void set_shards(EsShards _shards) {
    this._shards = _shards;
  }

  public boolean isFound() {
    return found;
  }

  public void setFound(boolean found) {
    this.found = found;
  }

  public String get_index() {
    return _index;
  }

  public void set_index(String _index) {
    this._index = _index;
  }

  public String get_type() {
    return _type;
  }

  public void set_type(String _type) {
    this._type = _type;
  }

  public String get_id() {
    return _id;
  }

  public void set_id(String _id) {
    this._id = _id;
  }

  public String get_version() {
    return _version;
  }

  public void set_version(String _version) {
    this._version = _version;
  }

}
