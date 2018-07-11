package com.alex.space.elastic.service;

import com.alex.space.common.utils.HttpClientUtil;
import com.alex.space.elastic.config.ElasticConfig;
import com.alex.space.elastic.config.ElasticConstants;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;


/**
 * Elastic node host checker
 *
 * @author Alex Created by Alex on 2018/7/10.
 */
@Slf4j
public class ElasticNodeHostChecker {

  private static ElasticNodeHostChecker instance = null;

  private final static Object LOCK_OBJECT = new Object();

  private ElasticConfig conf = ElasticConfig.getInstance();

  private String nodeHost = "";

  public static ElasticNodeHostChecker getInstance() {
    if (null == instance) {
      synchronized (LOCK_OBJECT) {
        if (null == instance) {
          instance = new ElasticNodeHostChecker();
          try {
            instance.checkEsNodeHost();
          } catch (Exception e) {
            log.error("first checkEsNodeHost failed!", e);
          }
        }
      }
    }
    return instance;
  }

  public String getEsNodeHost() {
    return nodeHost;
  }

  private void checkEsNodeHost() throws Exception {
    String nodeIps = conf.getProperty(ElasticConstants.ES_NODE_IPS);
    String[] nodes = nodeIps.split(",");

    for (int i = 0; i < nodes.length; i++) {
      String node = nodes[i];
      if (nodeHost.equals(node)) {
        continue;
      }
      Map map;
      try {
        map = HttpClientUtil.get("http://" + node, Map.class);
      } catch (Exception e) {
        log.error("es连接失败 node:" + node, e);
        continue;
      }
      if (map != null) {
        log.info("ES response : " + map);
        nodeHost = node;
        return;
      }
    }
    Map map = null;
    try {
      map = HttpClientUtil.get("http://" + nodeHost, Map.class);
    } catch (Exception e) {
      log.error("es连接失败 node:" + nodeHost, e);
    }
    if (map != null) {
      log.info("ES response : " + map);
      return;
    }

    throw new Exception("checkEsNodeHost all failed!  nodeIps: " + nodeIps);
  }

  public interface Handler<T> {

    T handle();
  }

  public <T> T doHandle(Handler<T> handler) {
    try {
      return handler.handle();
    } catch (Exception e) {
      log.warn("------ES request error------ checkEsNodeHost ", e);
      try {
        this.checkEsNodeHost();
        return handler.handle();
      } catch (Throwable e1) {
        throw new RuntimeException("try checkEsNodeHost failed", e1);
      }
    }
  }
}
