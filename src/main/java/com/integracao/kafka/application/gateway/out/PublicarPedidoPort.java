package com.integracao.kafka.application.gateway.out;

import com.integracao.kafka.domain.entity.Pedido;

public interface PublicarPedidoPort {
    
    void publicar(String topico, Pedido evento);
}
    