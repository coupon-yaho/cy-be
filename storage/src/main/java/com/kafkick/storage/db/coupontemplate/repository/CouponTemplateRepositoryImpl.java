package com.kafkick.storage.db.coupontemplate.repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import jakarta.persistence.EntityManager;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupontemplate.exception.CouponTemplatePersistenceException;
import com.kafkick.core.coupontemplate.query.CouponTemplatePage;
import com.kafkick.core.coupontemplate.port.CouponTemplateRepository;
import com.kafkick.storage.db.coupontemplate.entity.CouponTemplateEntity;
import com.kafkick.storage.db.coupontemplate.mapper.CouponTemplateEntityMapper;

@Repository
public class CouponTemplateRepositoryImpl implements CouponTemplateRepository {

    private final CouponTemplateJpaRepository jpaRepository;
    private final EntityManager entityManager;

    public CouponTemplateRepositoryImpl(
            CouponTemplateJpaRepository jpaRepository,
            EntityManager entityManager
    ) {
        this.jpaRepository = jpaRepository;
        this.entityManager = entityManager;
    }

    @Override
    public CouponTemplate save(CouponTemplate couponTemplate) {
        try {
            CouponTemplateEntity entity =
                    CouponTemplateEntityMapper.toEntity(couponTemplate);

            CouponTemplateEntity savedEntity =
                    jpaRepository.saveAndFlush(entity);

            entityManager.refresh(savedEntity);

            return CouponTemplateEntityMapper.toDomain(savedEntity);
        } catch (DataIntegrityViolationException exception) {
            throw new CouponTemplatePersistenceException(
                    "쿠폰 템플릿 저장 중 DB 제약 위반: brandId="
                            + couponTemplate.brandId(),
                    exception
            );
        }
    }

    @Override
    public Optional<CouponTemplate> findById(Long id) {
        return jpaRepository.findById(id)
                .map(CouponTemplateEntityMapper::toDomain);
    }

    @Override
    public List<CouponTemplate> findAllActiveByIdAsc() {
        return jpaRepository
                .findAllByActiveTrueAndPolicyTypeInOrderByIdAsc(
                        Set.of(CouponPolicyType.values())
                )
                .stream()
                .map(CouponTemplateEntityMapper::toDomain)
                .toList();
    }

    @Override
    public CouponTemplatePage findPageByIdAsc(int page, int size) {
        PageRequest pageRequest = PageRequest.of(
                page,
                size,
                Sort.by(Sort.Direction.ASC, "id")
        );
        Page<CouponTemplateEntity> entityPage =
                jpaRepository.findAll(pageRequest);

        return new CouponTemplatePage(
                entityPage.getContent().stream()
                        .map(CouponTemplateEntityMapper::toDomain)
                        .toList(),
                entityPage.getNumber(),
                entityPage.getSize(),
                entityPage.getTotalElements(),
                entityPage.getTotalPages()
        );
    }
}
