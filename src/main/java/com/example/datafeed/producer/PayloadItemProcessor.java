package com.example.datafeed.producer;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;


public class PayloadItemProcessor implements ItemProcessor<String, String> {

    private static final Logger log = LoggerFactory.getLogger(PayloadItemProcessor.class);

    @Override
    public String process(final String payload) throws Exception {

        log.info(payload.toString());
        
        MultiValueMap<String, String> headers = new LinkedMultiValueMap<String, String>();
        Map<String, String> map = new HashMap<String, String>();
        map.put("Content-Type", "application/json");

        headers.setAll(map);

        Map<String, String> requestPayload = new HashMap<String, String>();
        requestPayload.put("payload", payload);

        HttpEntity<?> request = new HttpEntity<>(requestPayload, headers);
        String url = "http://localhost:8080/datafeed/";

        ResponseEntity<?> response = new RestTemplate().postForEntity(url, request, String.class);

        System.out.println(response.getStatusCode());
        System.out.println(response.getBody());

        if (response.getStatusCode() != HttpStatus.OK) return payload;  //will be written to error file
        
        return null;
    }
}
