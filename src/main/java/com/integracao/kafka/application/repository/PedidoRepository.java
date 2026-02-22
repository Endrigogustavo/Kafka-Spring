package com.integracao.kafka.application.repository;

import java.util.List;

import com.integracao.kafka.domain.entity.PedidoEntity;

public interface PedidoRepository {
    
    PedidoEntity save(PedidoEntity pedidoEntity) ;
   
    PedidoEntity findById(Long id);

    List<PedidoEntity> findAll();
}
