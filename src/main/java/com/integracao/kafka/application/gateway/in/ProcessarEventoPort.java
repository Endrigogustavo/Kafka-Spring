package com.integracao.kafka.application.gateway.in;

import com.integracao.kafka.domain.model.Evento;

/**
 * Port de entrada (Hexagonal).
 * Define o contrato que o adaptador de entrada deve chamar.
 */
public interface ProcessarEventoPort {

    void executar(Evento evento);
}
