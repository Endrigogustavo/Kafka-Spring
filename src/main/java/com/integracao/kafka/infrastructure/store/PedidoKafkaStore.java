package com.integracao.kafka.infrastructure.store;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.integracao.kafka.domain.model.Pedido;

@Component
public class PedidoKafkaStore {

    private static final int MAX_ITEMS = 500;
    private final Deque<Pedido> pedidos = new ArrayDeque<>();

    public synchronized void adicionar(Pedido pedido) {
        if (pedidos.size() >= MAX_ITEMS) {
            pedidos.removeFirst();
        }
        pedidos.addLast(pedido);
    }

    public synchronized List<Pedido> listarTodos() {
        return new ArrayList<>(pedidos);
    }

    public synchronized Optional<Pedido> buscarPorNumeroPedido(String numeroPedido) {
        return pedidos.stream()
            .filter(pedido -> pedido.getNumeroPedido() != null && pedido.getNumeroPedido().equalsIgnoreCase(numeroPedido))
            .findFirst();
    }

    public synchronized void limpar() {
        pedidos.clear();
    }

    public synchronized int contarPedidos() {
        return pedidos.size();
    }
}