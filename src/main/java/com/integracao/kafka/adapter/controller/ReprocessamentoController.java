package com.integracao.kafka.adapter.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.integracao.kafka.application.useCase.subscribe.GerenciarFalhasUseCase;
import com.integracao.kafka.domain.model.FalhaProcessamento;
import com.integracao.kafka.domain.model.FalhaProcessamento.StatusFalha;
import com.integracao.kafka.domain.model.FalhaProcessamento.TipoFalha;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/reprocessamento")
@RequiredArgsConstructor
@Tag(name = "Reprocessamento", description = "Consulta de falhas e reprocessamento manual com validação")
public class ReprocessamentoController {

    private final GerenciarFalhasUseCase gerenciarFalhasUseCase;

    @GetMapping("/falhas")
    @Operation(summary = "Listar falhas", description = "Lista falhas de validação de pedido/nota para análise e reprocessamento")
    public ResponseEntity<List<FalhaProcessamento>> listarFalhas(
        @RequestParam(required = false) TipoFalha tipo,
        @RequestParam(defaultValue = "PENDENTE_REPROCESSAMENTO") StatusFalha status,
        @RequestParam(defaultValue = "100") int limite
    ) {
        return ResponseEntity.ok(gerenciarFalhasUseCase.listarFalhas(tipo, status, limite));
    }

    @PostMapping("/falhas/{id}/reprocessar")
    @Operation(summary = "Reprocessar falha", description = "Após validação, republica evento para tópico de entrada e marca como reprocessado")
    public ResponseEntity<Map<String, Object>> reprocessar(@PathVariable String id) {
        FalhaProcessamento falha = gerenciarFalhasUseCase.reprocessar(id);
        log.info("[REPROCESSAMENTO] Falha reprocessada manualmente | id={} tipo={}", falha.getId(), falha.getTipo());

        return ResponseEntity.ok(Map.of(
            "status", "sucesso",
            "mensagem", "Falha enviada para reprocessamento",
            "idFalha", falha.getId(),
            "tipo", falha.getTipo().name()
        ));
    }

    @PostMapping("/falhas/{id}/descartar")
    @Operation(summary = "Descartar falha", description = "Marca falha como descartada quando não deve ser reprocessada")
    public ResponseEntity<Map<String, Object>> descartar(@PathVariable String id) {
        FalhaProcessamento falha = gerenciarFalhasUseCase.descartar(id);

        return ResponseEntity.ok(Map.of(
            "status", "sucesso",
            "mensagem", "Falha descartada",
            "idFalha", falha.getId(),
            "tipo", falha.getTipo().name()
        ));
    }
}
