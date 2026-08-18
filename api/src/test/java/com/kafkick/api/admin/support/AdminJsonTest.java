package com.kafkick.api.admin.support;

import com.kafkick.ApiApplication;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.boot.test.autoconfigure.json.JsonTest;
import org.springframework.test.context.ContextConfiguration;

/** 실제 관리자 HTTP 응답과 같은 Jackson null 제외 설정을 사용하는 JSON 테스트 슬라이스입니다. */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@JsonTest(properties = "spring.jackson.default-property-inclusion=non_null")
@ContextConfiguration(classes = ApiApplication.class)
public @interface AdminJsonTest {
}
