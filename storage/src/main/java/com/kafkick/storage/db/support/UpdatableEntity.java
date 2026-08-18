package com.kafkick.storage.db.support;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;

import org.springframework.data.annotation.LastModifiedDate;

@MappedSuperclass
public abstract class UpdatableEntity extends BaseEntity {

    @LastModifiedDate
    @Column(nullable = false)
    private Instant updatedAt;

    protected UpdatableEntity() {
    }

    protected UpdatableEntity(
            Long id,
            Instant createdAt,
            Instant updatedAt
    ) {
        super(id, createdAt);
        this.updatedAt = updatedAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
