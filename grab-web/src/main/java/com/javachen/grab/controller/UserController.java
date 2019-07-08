package com.javachen.grab.controller;

import com.javachen.grab.model.CommonResponse;
import com.javachen.grab.model.domain.User;
import com.javachen.grab.model.request.LoginUserRequest;
import com.javachen.grab.model.request.RegisterUserRequest;
import com.javachen.grab.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Arrays;


@RequestMapping(value = "/rest/users")
@Controller
public class UserController {

    @Autowired
    private UserService userService;

    @RequestMapping(value = "/login",produces = MediaType.APPLICATION_JSON_VALUE,  method = RequestMethod.GET )
    @ResponseBody
    public CommonResponse login(@RequestParam("username") String username, @RequestParam("password") String password) {
        User user  =userService.loginUser(new LoginUserRequest(username,password));
        return CommonResponse.success(user);
    }

    @RequestMapping(value = "/register",produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.GET)
    @ResponseBody
    public CommonResponse addUser(@RequestParam("username") String username, @RequestParam("password") String password) {
        if(userService.checkUserExist(username)){
            return CommonResponse.error("用户名已存在");
        }
        userService.registerUser(new RegisterUserRequest(username,password));
        return CommonResponse.success();
    }

    //冷启动问题
    @RequestMapping(value = "/pref",produces = MediaType.APPLICATION_JSON_VALUE,  method = RequestMethod.GET)
    @ResponseBody
    public CommonResponse addPrefGenres(@RequestParam("username") String username,@RequestParam("genres") String genres) {
        User user = userService.findByUsername(username);
        user.getPrefGenres().addAll(Arrays.asList(genres.split(",")));
        user.setFirst(false);
        userService.updateUser(user);
        return CommonResponse.success();
    }
}
