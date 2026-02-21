package com.integracao.kafka.adapter.dto.request;

public record PedidoDtoRequest(
    String numeroPedido,
    String cliente,
    String produto,
    Integer quantidade,
    String valorTotal
) {}
