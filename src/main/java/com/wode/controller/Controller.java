package com.wode.controller;

import com.wode.util.RedisTemplateUtil;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Date;

/**
 * @description:
 * @author: jitao
 * @createDate: 2021/7/19
 */
@RestController
@RequestMapping("/redis")
public class Controller {

    @Resource
    private RedisTemplateUtil redisTemplateUtil;

    @RequestMapping("/set")
    public String setRedis(String key) {
        boolean set = redisTemplateUtil.set(key, key + System.currentTimeMillis());
        return set + "";
    }

    @RequestMapping("/get")
    public String getRedis(String key) {
        String s = redisTemplateUtil.get(key);
        return s;
    }
}
