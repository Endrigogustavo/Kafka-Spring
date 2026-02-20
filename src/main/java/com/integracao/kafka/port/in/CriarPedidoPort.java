package com.integracao.kafka.port.in;

import com.integracao.kafka.domain.model.Pedido;

/**
 * Port de entrada para criação de pedidos.
 * 
 * Define o contrato para criar e publicar pedidos no Kafka.
 * Implementado pelo use case de aplicação.
 */
public interface CriarPedidoPort {
    
    /**
     * Cria e publica um pedido no tópico Kafka.
     * 
     * @param pedido Dados do pedido a ser criado
     * @return ID do evento gerado
     * @throws IllegalArgumentException se dados do pedido forem inválidos
     * @throws RuntimeException se houver erro na publicação
     */
    String executar(Pedido pedido);
}
