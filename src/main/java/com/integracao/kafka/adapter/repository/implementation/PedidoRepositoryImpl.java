package com.integracao.kafka.adapter.repository.implementation;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.integracao.kafka.adapter.repository.iRepository.IPedidoRepository;
import com.integracao.kafka.application.repository.PedidoRepository;
import com.integracao.kafka.domain.entity.PedidoEntity;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class PedidoRepositoryImpl implements PedidoRepository {

    private final IPedidoRepository pedidoRepository;

    public PedidoEntity save(PedidoEntity pedidoEntity) {
        return pedidoRepository.save(pedidoEntity);
    }

    public PedidoEntity findById(Long id) {
        return pedidoRepository.findById(id).orElse(null);
    }

    public List<PedidoEntity> findAll() {
        return pedidoRepository.findAll();
    }
    
}
