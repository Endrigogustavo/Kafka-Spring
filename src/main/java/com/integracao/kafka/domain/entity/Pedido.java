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
public class Pedido {

    // Dados do pedido
    private String numeroPedido;
    private String cliente;
    private String produto;
    private Integer quantidade;
    private BigDecimal valorTotal;
    
    @Builder.Default
    private LocalDateTime dataPedido = LocalDateTime.now();
    
    // Metadados de processamento Kafka (SOMENTE LEITURA - computados automaticamente pelo subscriber)
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
 
    
    public void preencherMetadadosMocadosSeNecessario() {
        if (this.dataProcessamento == null) {
            this.dataProcessamento = LocalDateTime.now();
        }
        if (this.statusProcessamento == null) {
            this.statusProcessamento = "MOCK_PENDENTE";
        }
        if (this.kafkaOffset == null) {
            this.kafkaOffset = 0L;
        }
        if (this.kafkaPartition == null) {
            this.kafkaPartition = 0;
        }
        if (this.kafkaTopic == null) {
            this.kafkaTopic = "pedidos.criados";
        }
        if (this.kafkaTimestamp == null) {
            this.kafkaTimestamp = Instant.now();
        }
    }
}
