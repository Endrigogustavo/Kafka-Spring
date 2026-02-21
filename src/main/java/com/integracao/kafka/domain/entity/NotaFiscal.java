package com.integracao.kafka.domain.entity;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotaFiscal {
    private String numeroNota;
    private String numeroPedido;
    private String cliente;
    private String produto;
    private Integer quantidade;
    private BigDecimal valorTotal;

    @Builder.Default
    private LocalDateTime dataPedido = LocalDateTime.now();

    // Metadados de processamento Kafka (SOMENTE LEITURA - computados
    // automaticamente pelo subscriber)
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private LocalDateTime dataProcessamento;

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private String statusProcessamento;

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private Long kafkaOffset;

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private Integer kafkaPartition;

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private String kafkaTopic;

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private Instant kafkaTimestamp;
}
