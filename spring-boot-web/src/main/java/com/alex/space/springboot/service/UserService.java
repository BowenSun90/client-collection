package com.alex.space.springboot.service;

import com.alex.space.springboot.dao.UserMapper;
import com.alex.space.springboot.model.User;
import com.alex.space.springboot.property.GuavaCacheProperties;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Application
 *
 * @author Alex Created by Alex on 2017/06/02
 */
@Slf4j
@Service
public class UserService {

  @Autowired
  private UserMapper userMapper;

  private LoadingCache<String, List<User>> cityUserCache = null;

  private Cache<String, String> userNameCache = null;

  public UserService(GuavaCacheProperties cacheConfig) {

    cityUserCache = CacheBuilder.newBuilder().maximumSize(cacheConfig.getMaximumSize())
        .expireAfterWrite(cacheConfig.getDuration(), TimeUnit.valueOf(cacheConfig.getUnit()))
        .build(new CacheLoader<String, List<User>>() {

          @Override
          public List<User> load(String city) throws Exception {
            return userMapper.selectByCity(city);
          }

        });

    userNameCache = CacheBuilder.newBuilder().maximumSize(cacheConfig.getMaximumSize())
        .expireAfterWrite(cacheConfig.getDuration(), TimeUnit.valueOf(cacheConfig.getUnit()))
        .build();
  }

  public List<User> getCityUserList(String city) {
    log.info("getCityUserList({})", city);
    try {
      return cityUserCache.get(city);
    } catch (ExecutionException e1) {
      log.error("获取缓存失败,key={},error={}", city, e1.getMessage());
      return null;
    }
  }

  public boolean insertUser(String name, int code, String city) {
    log.info("insertUser({}, {}, {})", name, code, city);
    cityUserCache.invalidate(city);
    return userMapper.insert(name, code, city) == 1;
  }

  public boolean deleteUser(int code) {
    log.info("deleteUser({})", code);
    cityUserCache.invalidate(code);
    return userMapper.delete(code) == 1;
  }

  public boolean updateUser(int code, String city) {
    log.info("updateUser({}, {})", code, city);
    return userMapper.update(code, city) == 1;
  }

}
