package com.integracao.kafka.application.gateway.in;

import com.integracao.kafka.domain.entity.NotaFiscal;

public interface  PublicarNotaPort {
    String executar(NotaFiscal notaFiscal);
}
