package com.integracao.kafka.application.repository;

import java.util.List;

import com.integracao.kafka.domain.entity.NotaFiscalEntity;

public interface NotaFiscalRepository {
    NotaFiscalEntity save(NotaFiscalEntity notaFiscalEntity);
    NotaFiscalEntity findById(Long id);
    List<NotaFiscalEntity> findAll();
}
