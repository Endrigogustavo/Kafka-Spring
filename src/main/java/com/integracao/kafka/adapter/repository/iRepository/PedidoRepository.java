package com.integracao.kafka.adapter.repository.iRepository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.integracao.kafka.domain.entity.PedidoEntity;

public interface PedidoRepository extends JpaRepository<PedidoEntity, Long> {
}