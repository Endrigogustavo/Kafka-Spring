package com.integracao.kafka.adapter.controller;

import java.math.BigDecimal;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.integracao.kafka.adapter.dto.request.PedidoDtoRequest;
import com.integracao.kafka.application.useCase.publish.PublicarPedidoUseCase;
import com.integracao.kafka.domain.entity.Pedido;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/pedidos")
@RequiredArgsConstructor
@Tag(name = "Pedidos", description = "Endpoints para criação de pedidos no Kafka")
public class PedidoController {

    private final PublicarPedidoUseCase criarPedidoUseCase;

    @PostMapping
    @Operation(summary = "Criar novo pedido", description = "Cria um pedido e publica no tópico Kafka 'entrada.pedido' para processamento")
    public ResponseEntity<Map<String, Object>> criarPedido(@RequestBody PedidoDtoRequest pedido) {
        validarRequisicao(pedido);
        log.info("[API] Recebendo pedido | cliente={} produto={}", pedido.cliente(), pedido.produto());

        try {
            Pedido pedidoEntity = Pedido.builder()
                    .numeroPedido(pedido.numeroPedido())
                    .cliente(pedido.cliente())
                    .produto(pedido.produto())
                    .quantidade(pedido.quantidade())
                    .valorTotal(parseValorTotal(pedido.valorTotal()))
                    .build();

            String eventoId = criarPedidoUseCase.executar(pedidoEntity);
            
            log.info("[API] Pedido criado com sucesso | numero={} eventoId={}", 
                    pedido.numeroPedido(), eventoId);

            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                    "status", "sucesso",
                    "mensagem", "Pedido criado e publicado no Kafka",
                    "numeroPedido", pedido.numeroPedido(),
                    "eventoId", eventoId
            ));
        } catch (IllegalArgumentException e) {
            log.warn("[API] Dados inválidos | erro={}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "status", "erro",
                    "mensagem", e.getMessage()
            ));
        } catch (Exception e) {
            log.error("[API] Erro ao criar pedido | erro={}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                    "status", "erro",
                    "mensagem", "Falha ao criar pedido: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/exemplo")
    @Operation(summary = "Exemplo de pedido", description = "Retorna um exemplo de estrutura de pedido para referência")
    public ResponseEntity<Pedido> exemploPedido() {
        Pedido exemplo = Pedido.builder()
                .cliente("João Silva")
                .produto("Notebook Dell XPS")
                .quantidade(1)
                .valorTotal(new java.math.BigDecimal("4500.00"))
                .build();
        
        // Preenche metadados mocados para visualização no Swagger
        exemplo.preencherMetadadosMocadosSeNecessario();
        
        return ResponseEntity.ok(exemplo);
    }

    private void validarRequisicao(PedidoDtoRequest pedido) {
        if (pedido == null) {
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
