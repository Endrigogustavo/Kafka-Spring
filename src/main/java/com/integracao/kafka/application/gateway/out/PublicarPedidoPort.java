package com.integracao.kafka.application.gateway.out;

import com.integracao.kafka.domain.model.Pedido;

public interface PublicarPedidoPort {
    
    void publicar(String topico, Pedido evento);
}
    