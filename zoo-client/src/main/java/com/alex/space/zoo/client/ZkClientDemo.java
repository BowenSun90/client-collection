package com.alex.space.zoo.client;

import com.alex.space.zoo.config.ZooConfig;
import com.alex.space.zoo.config.ZooConstants;
import com.alex.space.zoo.entity.User;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * ZKClient demo
 *
 * @author Alex Created by Alex on 2018/6/19.
 */
@Slf4j
public class ZkClientDemo {

  public static void main(String[] args) {
    // Create zookeeper client
    ZkClient zkClient = new ZkClient(ZooConfig.getInstance().getProperty("zk.nodes"),
        ZooConstants.CONNECTION_TIMEOUT);
    log.info("connected ok!");

    // Check path exists
    String path = "/test";
    boolean exist = zkClient.exists(path);
    log.info("path {} exists: {}", path, exist);

    // Delete path (with recursive)
    exist = zkClient.delete(path);
    log.info("delete {}: {} ", path, exist);

    exist = zkClient.deleteRecursive(path);
    log.info("delete {}: {} ", path, exist);

    // Create node
    User user = new User();
    user.setId(1);
    user.setName("testUser");
    String result = zkClient.create(path, user, CreateMode.EPHEMERAL);
    log.info("created path: " + result);

    // Update node
    user = new User();
    user.setId(2);
    user.setName("testUser2");
    zkClient.writeData(path, user);
    log.info("update path: " + path);

    // Read node
    Stat stat = new Stat();
    User data = zkClient.readData(path, stat);
    log.info("read path: {}, data: {} ", path, data);
    log.info("stat: {}", stat);

  }

}
