package com.integracao.kafka.adapter.repository.implementation;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.integracao.kafka.adapter.repository.iRepository.INotaFiscalRepository;
import com.integracao.kafka.application.repository.NotaFiscalRepository;
import com.integracao.kafka.domain.entity.NotaFiscalEntity;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor 
public class NotaFiscalRepositoryImpl implements NotaFiscalRepository {
    
    private final INotaFiscalRepository notaFiscalRepository;

    public NotaFiscalEntity save(NotaFiscalEntity notaFiscalEntity) {
        return notaFiscalRepository.save(notaFiscalEntity);
    }

    public NotaFiscalEntity findById(Long id) {
        return notaFiscalRepository.findById(id).orElse(null);
    }

    public List<NotaFiscalEntity> findAll() {
        return notaFiscalRepository.findAll();
    }
}
