package com.integracao.kafka.application.service;

import org.springframework.stereotype.Service;

import com.integracao.kafka.adapter.repository.iRepository.PedidoRepository;
import com.integracao.kafka.domain.entity.PedidoEntity;
import com.integracao.kafka.domain.model.Pedido;

import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class PedidoService {

    private final PedidoRepository pedidoRepository;

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

        log.info("[SERVICE-PEDIDO] Enviando pedido para banco de dados | numeroPedido={}", pedidoEntity.getNumeroPedido());
        PedidoEntity pedidoSalvo = pedidoRepository.save(pedidoEntity);
        log.info("[SERVICE-PEDIDO] Pedido persistido com sucesso | id={} numeroPedido={} cliente={}",
            pedidoSalvo.getId(), pedidoSalvo.getNumeroPedido(), pedidoSalvo.getCliente());

        return pedidoSalvo;
    }


    public List<PedidoEntity> listarPedidos() {
        log.info("[SERVICE-PEDIDO] Consultando pedidos no banco de dados | operacao=listarPedidos");
        List<PedidoEntity> pedidos = pedidoRepository.findAll();
        log.info("[SERVICE-PEDIDO] Consulta de pedidos finalizada | totalPedidos={}", pedidos.size());
        return pedidos;
    }
    
}
