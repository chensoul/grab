package com.javachen.grab.rest;

import com.javachen.grab.model.domain.User;
import com.javachen.grab.model.request.LoginUserRequest;
import com.javachen.grab.model.request.RegisterUserRequest;
import com.javachen.grab.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Arrays;


@RequestMapping("/rest/users")
@Controller
public class UserRestApi {

    @Autowired
    private UserService userService;

    @RequestMapping(value = "/login",  method = RequestMethod.GET )
    @ResponseBody
    public User login(@RequestParam("username") String username, @RequestParam("password") String password) {
        User user  =userService.loginUser(new LoginUserRequest(username,password));
        return user;
    }

    @RequestMapping(value = "/register",  method = RequestMethod.GET)
    @ResponseBody
    public Model addUser(@RequestParam("username") String username,@RequestParam("password") String password,Model model) {
        if(userService.checkUserExist(username)){
            model.addAttribute("success",false);
            model.addAttribute("message"," 用户名已经被注册！");
            return model;
        }
        userService.registerUser(new RegisterUserRequest(username,password));
        return model;
    }

    //冷启动问题
    @RequestMapping(value = "/pref",  method = RequestMethod.GET)
    @ResponseBody
    public Model addPrefGenres(@RequestParam("username") String username,@RequestParam("genres") String genres,Model model) {
        User user = userService.findByUsername(username);
        user.getPrefGenres().addAll(Arrays.asList(genres.split(",")));
        user.setFirst(false);
        userService.updateUser(user);
        return model;
    }
}
