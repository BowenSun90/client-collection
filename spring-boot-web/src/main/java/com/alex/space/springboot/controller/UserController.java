package com.alex.space.springboot.controller;

import com.alex.space.springboot.model.User;
import com.alex.space.springboot.pojo.Response;
import com.alex.space.springboot.service.UserService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * UserController
 *
 * @author Alex Created by Alex on 2017/06/02
 */
@Slf4j
@RestController
@RequestMapping("/user")
@Api(value = "user", description = "User service", tags = "user")
public class UserController {

  @Autowired
  private UserService userService;

  @CrossOrigin
  @GetMapping("/query")
  @ApiOperation(value = "Get user list of city.", notes = "")
  @ApiResponses({@ApiResponse(code = 200, message = "成功"),
      @ApiResponse(code = 400, message = "参数错误")})
  public Response query(
      @NotNull @ApiParam(value = "City", required = true) @RequestParam(value = "city", required = true) String city) {
    log.info("访问 /user/query, city={}", city);

    if (ObjectUtils.isEmpty(city)) {
      return Response.valueOf(Response.Status.E400_BAD_REQUEST);
    }

    List<User> list = userService.getCityUserList(city);

    return Response.valueOf(list);
  }

}
