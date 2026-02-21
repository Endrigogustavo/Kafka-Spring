package com.integracao.kafka.adapter.dto.request;

public record NotaDtoRequest(
    String numeroNota,
    String cliente,
    String produto,
    Integer quantidade,
    String valorTotal
) {
    
}
