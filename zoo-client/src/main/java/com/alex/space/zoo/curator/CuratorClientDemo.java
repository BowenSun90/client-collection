package com.alex.space.zoo.curator;

import com.alex.space.zoo.config.ZooConfig;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * Curator Utils
 *
 * @author Alex Created by Alex on 2018/6/20.
 */
@Slf4j
public class CuratorClientDemo {

  public static void main(String[] args) throws Exception {

    // Create curator client
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client = CuratorFrameworkFactory.builder()
        .connectString(ZooConfig.getInstance().getProperty("zk.nodes"))
        .sessionTimeoutMs(5000)
        .connectionTimeoutMs(5000)
        .retryPolicy(retryPolicy)
        .build();

    client.start();
    log.info("client start");

    String path = "/test/node2";

    // Check path
    Stat stat = client.checkExists().forPath(path);
    log.info("check path: {}, {}", path, stat);

    if (stat != null) {
      // Delete path
      client.delete().forPath(path);
      log.info("delete path: " + path);
    }

    // Create path
    String result = client.create().creatingParentsIfNeeded().forPath(path);
    log.info("created path: " + result);

    client.delete().deletingChildrenIfNeeded().forPath(path);

    result = client.create()
        .creatingParentContainersIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(path + "/data", "init".getBytes());
    log.info("created path: " + result);

    // Read path
    byte[] bytes = client.getData().forPath(result);
    log.info("read path: {}, {}", result, new String(bytes));

    // Update path
    client.setData().forPath(result, "data".getBytes());

    stat = new Stat();
    client.getData().storingStatIn(stat).forPath(result);
    log.info("read path: {}, {}", result, stat);

    stat = client.setData().withVersion(stat.getVersion()).forPath(result, "data2".getBytes());

    client.delete().withVersion(stat.getVersion()).forPath(result);

    // Transaction
    path = "/test";
    client.inTransaction().check().forPath(path)
        .and()
        .create().withMode(CreateMode.PERSISTENT).forPath(path + "/node4", "data".getBytes())
        .and()
        .setData().forPath(path + "/node4", "data2".getBytes())
        .and()
        .commit();

    // Scan path
    List<String> list = client.getChildren().forPath(path);
    list.forEach(System.out::println);

    // guaranteed()接口是一个保障措施，只要客户端会话有效，那么Curator会在后台持续进行删除操作，直到删除节点成功
    client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);

  }

}
