package com.integracao.kafka.domain.entity;

import java.time.LocalDateTime;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FalhaProcessamento {

    @Builder.Default
    private String id = UUID.randomUUID().toString();

    private TipoFalha tipo;
    private Evento eventoOriginal;
    private String motivo;
    private String topicoOrigem;
    private Integer particaoOrigem;
    private Long offsetOrigem;

    @Builder.Default
    private LocalDateTime criadoEm = LocalDateTime.now();

    private LocalDateTime reprocessadoEm;

    private LocalDateTime ultimaTentativaReprocessamentoEm;

    private LocalDateTime proximaTentativaPermitidaEm;

    @Builder.Default
    private Integer tentativasReprocessamento = 0;

    @Builder.Default
    private Integer maxTentativasReprocessamento = 5;

    @Builder.Default
    private StatusFalha status = StatusFalha.PENDENTE_REPROCESSAMENTO;

    public enum TipoFalha {
        PEDIDO,
        NOTA
    }

    public enum StatusFalha {
        PENDENTE_REPROCESSAMENTO,
        REPROCESSADO,
        ESGOTADO,
        DESCARTADO
    }
}
