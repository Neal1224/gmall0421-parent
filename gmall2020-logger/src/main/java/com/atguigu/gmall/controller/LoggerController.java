package com.atguigu.gmall.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@Slf4j

public class LoggerController {


    @Autowired //(KafkaTemplate kafkaTemplate = new KafkaTemplate)
    KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")


    public String applog(@RequestBody String jsonLog) {
        //将日志落盘
        log.info(jsonLog);
        //将不同类型日志发送到kafka主题中
        JSONObject jsonObject = JSON.parseObject(jsonLog);
        if (jsonObject.getString("start") != null) {
            //启动日志
            kafkaTemplate.send("gmall_start_bak", jsonLog);
        } else {
            //事件日志
            kafkaTemplate.send("gmall_event_bak", jsonLog);
        }
        return "success";
    }
}