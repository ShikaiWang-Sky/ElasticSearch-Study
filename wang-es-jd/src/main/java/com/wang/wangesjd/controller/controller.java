package com.wang.wangesjd.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class controller {

    @RequestMapping({"/", "/index"})
    public String index() {
        return "index";
    }

}