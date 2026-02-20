package com.integracao.kafka.port.out;

import com.integracao.kafka.domain.model.Evento;

/**
 * Port de saída (Hexagonal).
 * Define o contrato que o adaptador de saída deve implementar.
 */
public interface PublicarEventoPort {

    void publicar(String topico, Evento evento);
}
