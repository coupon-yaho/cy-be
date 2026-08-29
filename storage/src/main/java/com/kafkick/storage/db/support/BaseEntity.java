package com.kafkick.storage.db.support;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@MappedSuperclass
@EntityListeners(AuditingEntityListener.class)
public abstract class BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @CreatedDate
    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    protected BaseEntity() {
    }

    protected BaseEntity(Long id, Instant createdAt) {
        this.id = id;
        this.createdAt = createdAt;
    }

    public Long getId() {
        return id;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
