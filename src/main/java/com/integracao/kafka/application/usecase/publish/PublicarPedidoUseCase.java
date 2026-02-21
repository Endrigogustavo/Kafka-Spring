package com.integracao.kafka.application.useCase.publish;

import java.util.UUID;

import org.springframework.stereotype.Service;

import com.integracao.kafka.application.gateway.in.PublicarPedidoPort;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.application.metrics.IntegradorMetrics;
import com.integracao.kafka.domain.entity.Evento;
import com.integracao.kafka.domain.entity.Pedido;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PublicarPedidoUseCase implements PublicarPedidoPort {

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
