package com.kafkick.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// API와 연결된 Core 및 Storage 모듈의 Spring 컴포넌트를 검색합니다.
@SpringBootApplication(scanBasePackages = "com.kafkick")
public class ApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }

}
