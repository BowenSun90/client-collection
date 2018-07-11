package com.alex.space.elastic.entity;

import java.util.List;
import java.util.Map;

public class EsHitsRecord {

  private String _index;
  private String _type;
  private String _id;
  private List<String> sort;
  private double _score;
  private Map<String, Object> _source;

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

  public double get_score() {
    return _score;
  }

  public void set_score(double _score) {
    this._score = _score;
  }

  public Map<String, Object> get_source() {
    return _source;
  }

  public void set_source(Map<String, Object> _source) {
    this._source = _source;
  }

  public List<String> getSort() {
    return sort;
  }

  public void setSort(List<String> sort) {
    this.sort = sort;
  }


}
