package com.integracao.kafka.frameworkDrivers.kafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.integracao.kafka.application.gateway.out.PublicarEventoPort;
import com.integracao.kafka.application.repository.PedidoRepository;
import com.integracao.kafka.application.service.PedidoService;
import com.integracao.kafka.application.useCase.subscribe.GerenciarFalhasUseCase;
import com.integracao.kafka.application.useCase.subscribe.ReceberPedidoUseCase;
import com.integracao.kafka.domain.entity.PedidoEntity;
import com.integracao.kafka.domain.model.Evento;
import com.integracao.kafka.domain.model.FalhaProcessamento.TipoFalha;
import com.integracao.kafka.domain.model.Pedido;

class PedidoConsumerTest {

    private TestPublicarEventoPort publicarEventoPort;
    private ReceberPedidoUseCase receberPedidoUseCase;
    private GerenciarFalhasUseCase gerenciarFalhasUseCase;
    private PedidoService pedidoService;
    private TestAcknowledgment acknowledgment;

    private PedidoConsumer consumer;

    @BeforeEach
    void setUp() {
        publicarEventoPort = new TestPublicarEventoPort();
        receberPedidoUseCase = new ReceberPedidoUseCase(100);
        gerenciarFalhasUseCase = new GerenciarFalhasUseCase(
            publicarEventoPort,
            "integrador.pedido.recebido",
            "integrador.nota.recebido",
            100,
            3,
            1
        );
        pedidoService = new PedidoServiceSempreFalha();
        acknowledgment = new TestAcknowledgment();

        consumer = new PedidoConsumer(
            publicarEventoPort,
            new ObjectMapper().findAndRegisterModules(),
            receberPedidoUseCase,
            gerenciarFalhasUseCase,
            pedidoService
        );

        ReflectionTestUtils.setField(consumer, "topicoSaidaPedido", "integrador.pedido.processado");
        ReflectionTestUtils.setField(consumer, "topicoDlqPedido", "integrador.pedido.dlq");
        ReflectionTestUtils.setField(consumer, "topicoRetryPedido", "integrador.pedido.retry");
        ReflectionTestUtils.setField(consumer, "maxTentativasRetry", 3);
    }

    @Test
    void deveReenviarParaRetryQuandoFalharNoReprocessamentoEAindaNaoEsgotouTentativas() {
        Evento evento = criarEventoPedidoValido();
        evento.setTentativasRetry(1);

        ConsumerRecord<String, Evento> record = new ConsumerRecord<>(
            "integrador.pedido.retry", 0, 15L, "key-1", evento
        );

        consumer.reprocessarPedidoComErro(record, acknowledgment);

        assertEquals(1, publicarEventoPort.publicacoes.size());
        assertEquals("integrador.pedido.retry", publicarEventoPort.publicacoes.get(0).topico());
        Evento reenviado = publicarEventoPort.publicacoes.get(0).evento();
        assertEquals(2, reenviado.getTentativasRetry());
        assertEquals(Evento.StatusEvento.FALHA, reenviado.getStatus());
        assertTrue(acknowledgment.acknowledged);
        assertTrue(gerenciarFalhasUseCase.listarFalhas(TipoFalha.PEDIDO, null, 100).isEmpty());
    }

    @Test
    void deveEnviarParaDlqQuandoFalharNoReprocessamentoEAtingirLimite() {
        Evento evento = criarEventoPedidoValido();
        evento.setTentativasRetry(3);

        ConsumerRecord<String, Evento> record = new ConsumerRecord<>(
            "integrador.pedido.retry", 0, 16L, "key-2", evento
        );

        consumer.reprocessarPedidoComErro(record, acknowledgment);

        assertEquals(1, publicarEventoPort.publicacoes.size());
        assertEquals("integrador.pedido.dlq", publicarEventoPort.publicacoes.get(0).topico());
        assertEquals(evento, publicarEventoPort.publicacoes.get(0).evento());
        assertTrue(acknowledgment.acknowledged);
        assertEquals(1, gerenciarFalhasUseCase.listarFalhas(TipoFalha.PEDIDO, null, 100).size());
    }

    private Evento criarEventoPedidoValido() {
        Pedido pedido = Pedido.builder()
            .numeroPedido("PED-100")
            .cliente("Cliente Teste")
            .produto("Produto X")
            .quantidade(1)
            .valorTotal(new BigDecimal("99.90"))
            .build();

        return Evento.builder()
            .tipo("PEDIDO_CRIADO")
            .origem("TESTE")
            .destino("SISTEMA_PEDIDOS")
            .payload(pedido)
            .status(Evento.StatusEvento.RECEBIDO)
            .tentativasRetry(1)
            .build();
    }

    private record Publicacao(String topico, Evento evento) {
    }

    private static class TestPublicarEventoPort implements PublicarEventoPort {
        private final List<Publicacao> publicacoes = new ArrayList<>();

        @Override
        public void publicar(String topico, Evento evento) {
            publicacoes.add(new Publicacao(topico, evento));
        }
    }

    private static class TestAcknowledgment implements Acknowledgment {
        private boolean acknowledged;

        @Override
        public void acknowledge() {
            this.acknowledged = true;
        }
    }

    private static class PedidoServiceSempreFalha extends PedidoService {
        PedidoServiceSempreFalha() {
            super(new PedidoRepository() {
                @Override
                public PedidoEntity save(PedidoEntity pedidoEntity) {
                    return pedidoEntity;
                }

                @Override
                public PedidoEntity findById(Long id) {
                    return null;
                }

                @Override
                public List<PedidoEntity> findAll() {
                    return List.of();
                }
            });
        }

        @Override
        public PedidoEntity criarPedido(Pedido pedido) {
            throw new RuntimeException("falha persistencia");
        }
    }
}
