package com.integracao.kafka.application.gateway.out;

import com.integracao.kafka.domain.entity.Evento;

/**
 * Port de saída (Hexagonal).
 * Define o contrato que o adaptador de saída deve implementar.
 */
public interface PublicarEventoPort {

    void publicar(String topico, Evento evento);
}
