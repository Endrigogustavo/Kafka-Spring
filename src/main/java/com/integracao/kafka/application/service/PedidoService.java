package com.integracao.kafka.application.service;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import com.integracao.kafka.adapter.repository.iRepository.PedidoRepository;
import com.integracao.kafka.domain.entity.PedidoEntity;
import com.integracao.kafka.domain.model.Pedido;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PedidoService {

    private final PedidoRepository pedidoRepository;

    @Value("${integrador.persistencia.retry-interval-ms:5000}")
    private long retryIntervalMs;

    @Value("${integrador.persistencia.retry-max-interval-ms:30000}")
    private long retryMaxIntervalMs;

    public PedidoEntity criarPedido(Pedido pedido) {
        log.info("[SERVICE-PEDIDO] Iniciando persistencia de pedido | numeroPedido={} cliente={} produto={} quantidade={} valorTotal={}",
            pedido.getNumeroPedido(), pedido.getCliente(), pedido.getProduto(), pedido.getQuantidade(), pedido.getValorTotal());
  
        PedidoEntity pedidoEntity = new PedidoEntity();
        pedidoEntity.setNumeroPedido(pedido.getNumeroPedido());
        pedidoEntity.setCliente(pedido.getCliente());
        pedidoEntity.setProduto(pedido.getProduto());
        pedidoEntity.setQuantidade(pedido.getQuantidade());
        pedidoEntity.setValorTotal(pedido.getValorTotal());

        log.info("[SERVICE-PEDIDO] Pedido mapeado para entidade | numeroPedido={} cliente={} produto={} quantidade={} valorTotal={}",
            pedidoEntity.getNumeroPedido(), pedidoEntity.getCliente(), pedidoEntity.getProduto(), pedidoEntity.getQuantidade(), pedidoEntity.getValorTotal());

        int tentativa = 1;
        while (true) {
            try {
                log.info("[SERVICE-PEDIDO] Enviando pedido para banco de dados | numeroPedido={} tentativa={}",
                    pedidoEntity.getNumeroPedido(), tentativa);

                PedidoEntity pedidoSalvo = pedidoRepository.save(pedidoEntity);

                log.info("[SERVICE-PEDIDO] Pedido persistido com sucesso | id={} numeroPedido={} cliente={} tentativa={} statusBanco=RECUPERADO",
                    pedidoSalvo.getId(), pedidoSalvo.getNumeroPedido(), pedidoSalvo.getCliente(), tentativa);

                return pedidoSalvo;
            } catch (DataAccessException ex) {
                long proximaTentativaMs = calcularBackoffComJitter(tentativa);

                log.error("[SERVICE-PEDIDO] Falha ao persistir pedido no banco | numeroPedido={} tentativa={} proximaTentativaEmMs={} erro={}",
                    pedidoEntity.getNumeroPedido(), tentativa, proximaTentativaMs, ex.getMessage());

                aguardarProximaTentativa(pedidoEntity.getNumeroPedido(), tentativa, proximaTentativaMs);

                tentativa++;
            }
        }
    }


    public List<PedidoEntity> listarPedidos() {
        log.info("[SERVICE-PEDIDO] Consultando pedidos no banco de dados | operacao=listarPedidos");
        List<PedidoEntity> pedidos = pedidoRepository.findAll();
        log.info("[SERVICE-PEDIDO] Consulta de pedidos finalizada | totalPedidos={}", pedidos.size());
        return pedidos;
    }

    private long calcularBackoffComJitter(int tentativaAtual) {
        long exponencial = retryIntervalMs * (1L << Math.min(tentativaAtual - 1, 10));
        long limitado = Math.min(exponencial, retryMaxIntervalMs);
        long jitter = ThreadLocalRandom.current().nextLong(250, 1000);
        return Math.min(limitado + jitter, retryMaxIntervalMs);
    }

    private void aguardarProximaTentativa(String numeroPedido, int tentativaAtual, long esperaMs) {
        try {
            Thread.sleep(esperaMs);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                "[SERVICE-PEDIDO] Thread interrompida durante espera de recuperação do banco | numeroPedido="
                    + numeroPedido + " tentativa=" + tentativaAtual + " esperaMs=" + esperaMs,
                ex
            );
        }
    }
    
}
