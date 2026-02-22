package com.integracao.kafka.application.useCase.subscribe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.integracao.kafka.domain.model.Pedido;

@Service
public class ReceberPedidoUseCase {

	private final int limiteHistorico;
	private final ConcurrentLinkedDeque<Pedido> historico = new ConcurrentLinkedDeque<>();

	public ReceberPedidoUseCase(@Value("${integrador.historico.pedidos.limite:500}") int limiteHistorico) {
		this.limiteHistorico = Math.max(1, limiteHistorico);
	}

	public void registrar(Pedido pedido) {
		if (pedido == null) {
			return;
		}

		historico.addLast(pedido);

		while (historico.size() > limiteHistorico) {
			historico.pollFirst();
		}
	}

	public List<Pedido> listarUltimos(int limite) {
		List<Pedido> itens = new ArrayList<>(historico);
		if (itens.isEmpty()) {
			return List.of();
		}

		int limiteAjustado = limite <= 0 ? itens.size() : Math.min(limite, itens.size());
		int inicio = Math.max(0, itens.size() - limiteAjustado);
		return List.copyOf(itens.subList(inicio, itens.size()));
	}
}
