package com.kafkick.storage;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * storage 는 라이브러리 모듈이라 실행 가능한 부트 앱이 없다.
 *
 * <p>@DataJpaTest 계열은 패키지를 거슬러 올라가며 @SpringBootConfiguration 을 찾는데,
 * 없으면 컨텍스트가 아예 뜨지 않는다. 이 클래스가 그 진입점 역할만 한다 — 테스트 전용이며
 * 프로덕션 산출물에는 들어가지 않는다.
 */
@SpringBootApplication
public class StorageTestApplication {
}
