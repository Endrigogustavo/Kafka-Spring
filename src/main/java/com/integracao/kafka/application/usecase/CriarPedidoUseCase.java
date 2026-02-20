package com.integracao.kafka.application.usecase;

import java.util.UUID;

import org.springframework.stereotype.Service;

import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.domain.model.Pedido;
import com.integracao.kafka.infrastructure.metrics.IntegradorMetrics;
import com.integracao.kafka.port.in.CriarPedidoPort;
import com.integracao.kafka.port.out.PublicarEventoPort;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Use Case para criação de pedidos.
 * 
 * Orquestra o fluxo completo:
 * 1. Valida dados do pedido
 * 2. Gera número do pedido se necessário
 * 3. Encapsula em Evento
 * 4. Publica no Kafka via porta de saída
 * 
 * Segue arquitetura hexagonal - não conhece tecnologias (HTTP, Kafka, etc)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CriarPedidoUseCase implements CriarPedidoPort {

    private final PublicarEventoPort publicarEventoPort;
    private final IntegradorMetrics metrics;
    
    private static final String TOPICO_ENTRADA_PEDIDO = "entrada.pedido";

    @Override
    public String executar(Pedido pedido) {
        log.info("[ORQUESTRADOR-PEDIDO] Iniciando criação de pedido | cliente={} produto={}", 
            pedido.getCliente(), pedido.getProduto());

        String eventoId;
        try {
            // Passo 1: Valida
            validar(pedido);

            // Passo 2: Gera número do pedido se não fornecido
            if (pedido.getNumeroPedido() == null || pedido.getNumeroPedido().isEmpty()) {
                pedido.setNumeroPedido("PED-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
            }
            
            // Passo 3: Preenche metadados mocados se necessário
            pedido.preencherMetadadosMocadosSeNecessario();

            // Passo 4: Encapsula em Evento
            Evento evento = Evento.builder()
                    .tipo("PEDIDO_CRIADO")
                    .origem("API_REST")
                    .destino("SISTEMA_PEDIDOS")
                    .payload(pedido)
                    .status(Evento.StatusEvento.RECEBIDO)
                    .build();

            // Passo 5: Publica via port de saída (com registro de tempo)
            metrics.registrarTempo(() -> {
                publicarEventoPort.publicar(TOPICO_ENTRADA_PEDIDO, evento);
            });
            
            eventoId = evento.getId();
            metrics.registrarSucesso();
            
            log.info("[ORQUESTRADOR-PEDIDO] Pedido criado com sucesso | numero={} eventoId={}", 
                pedido.getNumeroPedido(), eventoId);
            
            return eventoId;

        } catch (IllegalArgumentException ex) {
            metrics.registrarFalha();
            log.error("[ORQUESTRADOR-PEDIDO] Dados inválidos | erro={}", ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            metrics.registrarFalha();
            log.error("[ORQUESTRADOR-PEDIDO] Falha ao criar pedido | erro={}", ex.getMessage());
            throw new RuntimeException("Falha ao criar pedido: " + ex.getMessage(), ex);
        }
    }

    private void validar(Pedido pedido) {
        if (pedido == null) {
            throw new IllegalArgumentException("Pedido não pode ser nulo");
        }
        if (pedido.getValorTotal() == null) {
            throw new IllegalArgumentException("Valor total é obrigatório");
        }
        if (pedido.getValorTotal().signum() <= 0) {
            throw new IllegalArgumentException("Valor total deve ser maior que zero");
        }
        log.debug("[ORQUESTRADOR-PEDIDO] Pedido validado");
    }
}
