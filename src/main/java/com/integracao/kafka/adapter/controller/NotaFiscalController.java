package com.integracao.kafka.adapter.controller;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.integracao.kafka.adapter.dto.request.NotaDtoRequest; 
import com.integracao.kafka.application.useCase.publish.PublicarNotaFiscalUseCase;
import com.integracao.kafka.application.useCase.subscribe.ReceberNotaUseCase;
import com.integracao.kafka.domain.model.NotaFiscal;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/notas")
@RequiredArgsConstructor
@Tag(name = "Notas Fiscais", description = "Endpoints para criação de notas fiscais no Kafka")
public class NotaFiscalController {

    private final PublicarNotaFiscalUseCase criarNotaFiscalUseCase;
    private final ReceberNotaUseCase receberNotaUseCase;


    @PostMapping
    @Operation(summary = "Criar nova nota fiscal", description = "Cria uma nota fiscal e publica no tópico Kafka 'entrada.nota' para processamento")
    public ResponseEntity<Map<String, Object>> criarNotaFiscal(@RequestBody NotaDtoRequest nota) {
        validarRequisicao(nota);
        log.info("[API] Recebendo nota fiscal | cliente={} produto={}", nota.cliente(), nota.produto());

        try {
            NotaFiscal notaEntity = NotaFiscal.builder()
                    .numeroNota(nota.numeroNota())
                    .cliente(nota.cliente())
                    .produto(nota.produto())
                    .quantidade(nota.quantidade())
                    .valorTotal(parseValorTotal(nota.valorTotal()))
                    .build();

            String eventoId = criarNotaFiscalUseCase.executar(notaEntity);
            
            log.info("[API] Nota fiscal criada com sucesso | numero={} eventoId={}", 
                    nota.numeroNota(), eventoId);

            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                    "status", "sucesso",
                    "mensagem", "Nota fiscal criada e publicada no Kafka",
                    "numeroNota", nota.numeroNota(),
                    "eventoId", eventoId
            ));
        } catch (IllegalArgumentException e) {
            log.warn("[API] Dados inválidos | erro={}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "status", "erro",
                    "mensagem", e.getMessage()
            ));
        } catch (Exception e) {
            log.error("[API] Erro ao criar nota fiscal | erro={}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "erro",
                    "mensagem", "Falha ao criar nota fiscal : " + e.getMessage()
            ));
        }
    }

    @GetMapping("/consumidas")
    @Operation(summary = "Listar notas consumidas", description = "Retorna as últimas notas fiscais processadas pelo consumer")
    public ResponseEntity<List<NotaFiscal>> listarNotasConsumidas(@RequestParam(defaultValue = "50") int limite) {
        return ResponseEntity.ok(receberNotaUseCase.listarUltimas(limite));
    }

    private void validarRequisicao(NotaDtoRequest nota) {
        if (nota == null) {
            throw new IllegalArgumentException("Corpo da requisição é obrigatório");
        }
    }

    private BigDecimal parseValorTotal(String valorTotal) {
        if (valorTotal == null || valorTotal.isBlank()) {
            throw new IllegalArgumentException("Valor total é obrigatório");
        }
        try {
            return new BigDecimal(valorTotal);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Valor total inválido. Use formato numérico");
        }
    }
    
}
