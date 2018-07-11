package com.alex.space.zoo.client;

import com.alex.space.zoo.config.ZooConfig;
import com.alex.space.zoo.config.ZooConstants;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;

/**
 * Subscribe
 *
 * @author Alex Created by Alex on 2018/6/19.
 */
@Slf4j
public class SubscribeChild {

  /**
   * IZkChildListener implement
   */
  private static class ZkChildListener implements IZkChildListener {

    /**
     * 用来处理服务器端发送过来的通知
     *
     * @param parentPath 对应的父节点的路径
     * @param currentChildren 子节点的相对路径
     */
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren)
        throws Exception {
      log.info("parent: " + parentPath);

      log.info("children: " + currentChildren.toString());
    }

  }

  public static void main(String[] args) throws InterruptedException {
    ZkClient zkClient = new ZkClient(ZooConfig.getInstance().getProperty("zk.nodes"),
        ZooConstants.SESSION_TIMEOUT,
        ZooConstants.CONNECTION_TIMEOUT,
        new SerializableSerializer());
    log.info("connected ok!");

    // Subscribe
    String path = "/test/node1";
    zkClient.subscribeChildChanges(path, new ZkChildListener());
    Thread.sleep(1000000);
  }
}
