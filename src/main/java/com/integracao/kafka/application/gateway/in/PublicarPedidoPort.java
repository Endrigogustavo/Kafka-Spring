package com.integracao.kafka.application.gateway.in;

import com.integracao.kafka.domain.model.Pedido;

public interface PublicarPedidoPort {
    String executar(Pedido pedido);
}
