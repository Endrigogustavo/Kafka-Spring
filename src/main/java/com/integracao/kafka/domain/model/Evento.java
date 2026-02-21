package com.integracao.kafka.domain.model;

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
public class Evento {

    @Builder.Default
    private String id = UUID.randomUUID().toString();

    private String tipo;
    private String origem;
    private String destino;
    private Object payload;

    @Builder.Default
    private LocalDateTime criadoEm = LocalDateTime.now();

    private StatusEvento status;

    public enum StatusEvento {
        RECEBIDO,
        PROCESSANDO,
        ENVIADO,
        FALHA
    }
}
