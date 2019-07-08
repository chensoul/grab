package com.javachen.grab.rest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * @author june
 * @createTime 2019-07-08 01:02
 * @see
 * @since
 */
@Controller
public class IndexController {
    @RequestMapping(value = {"/"})
    public String index(){
        return "index";
    }
}
