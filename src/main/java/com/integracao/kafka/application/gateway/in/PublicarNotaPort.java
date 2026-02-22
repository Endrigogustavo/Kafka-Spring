package com.integracao.kafka.application.gateway.in;

import com.integracao.kafka.domain.model.NotaFiscal;

public interface  PublicarNotaPort {
    String executar(NotaFiscal notaFiscal);
}
