package com.integracao.kafka.adapter.repository.iRepository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.integracao.kafka.domain.entity.NotaFiscalEntity;

public interface INotaFiscalRepository extends JpaRepository<NotaFiscalEntity, Long> {
    
}
