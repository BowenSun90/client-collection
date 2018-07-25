package com.alex.space.springboot.dao;

import com.alex.space.springboot.model.User;
import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

/**
 * UserController
 *
 * @author Alex Created by Alex on 2017/06/02
 */
@Mapper
public interface UserMapper {

  List<User> selectByCity(String city);

  @Insert("insert into t_user (name, code, city)" +
      " values (#{name,jdbcType=VARCHAR}, #{code,jdbcType=INTEGER}, #{city,jdbcType=VARCHAR})")
  int insert(@Param("name") String name, @Param("code") int code, @Param("city") String city);

  @Delete("delete from t_user where code = #{code,jdbcType=INTEGER}")
  int delete(@Param("code") int code);

  @Update("UPDATE t_user SET city = #{city,jdbcType=VARCHAR} WHERE code = #{code,jdbcType=VARCHAR}")
  int update(@Param("code") int code, @Param("city") String city);
}
