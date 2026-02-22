# Kafka Integrador (Spring Boot + Kafka)

Integrador event-driven com arquitetura limpa para processamento de **Pedidos**, **Notas Fiscais** e **Eventos Genéricos**, com trilhas de **retry** e **DLQ**, além de observabilidade com Prometheus, Grafana e Loki.

## Stack

- Java 21
- Spring Boot 3.3.4
- Spring Kafka
- Spring Data JPA + H2 (in-memory)
- Resilience4j (CircuitBreaker, Retry, Bulkhead, RateLimiter)
- Actuator + Micrometer + Prometheus
- Grafana + Loki
- Docker Compose

## Arquitetura (atual)

Estrutura principal por camadas:

- `adapter`
  - Controllers REST e DTOs de entrada/saída.
- `application`
  - Use cases, gateways (ports), serviços de aplicação e métricas.
- `domain`
  - Modelos e entidades de negócio.
- `frameworkDrivers`
  - Kafka (consumers/producers), configurações e detalhes de infraestrutura.

### Fluxo resumido

1. API recebe pedido/nota.
2. Use case publica evento no tópico `integrador.<recurso>.recebido`.
3. Consumer processa, persiste e publica em `integrador.<recurso>.processado`.
4. Em falha técnica no processamento inicial: `DefaultErrorHandler` envia para `integrador.<recurso>.retry`.
5. Em falha durante reprocessamento (ou erro de validação): mensagem segue para `integrador.<recurso>.dlq`.
6. Falhas de pedido/nota ficam registradas em memória e podem ser reprocessadas/descartadas via API.

## Convenção de tópicos

Padrão: `integrador.<recurso>.<estado>`

- Domínio: `integrador`
- Recursos: `evento`, `pedido`, `nota`
- Estados: `recebido`, `processado`, `retry`, `dlq`

### Tópicos usados no projeto

| Recurso | Recebido | Processado | Retry | DLQ |
|---|---|---|---|---|
| evento | `integrador.evento.recebido` | `integrador.evento.processado` | `integrador.evento.retry` | `integrador.evento.dlq` |
| pedido | `integrador.pedido.recebido` | `integrador.pedido.processado` | `integrador.pedido.retry` | `integrador.pedido.dlq` |
| nota | `integrador.nota.recebido` | `integrador.nota.processado` | `integrador.nota.retry` | `integrador.nota.dlq` |

## Endereços e portas

Com `docker compose up -d`:

- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8090`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)
- Loki: `http://localhost:3100`

Aplicação Spring Boot:

- API: `http://localhost:8082`
- Swagger UI: `http://localhost:8082/swagger-ui.html`
- OpenAPI JSON: `http://localhost:8082/v3/api-docs`
- Actuator Health: `http://localhost:8082/actuator/health`
- Actuator Prometheus: `http://localhost:8082/actuator/prometheus`

## Como executar

### 1) Subir infraestrutura

```bash
docker compose up -d
```

### 2) Rodar aplicação

Windows (PowerShell):

```powershell
.\mvnw.cmd spring-boot:run
```

Linux/Mac:

```bash
./mvnw spring-boot:run
```

### 3) Rodar testes

Windows:

```powershell
.\mvnw.cmd test
```

Linux/Mac:

```bash
./mvnw test
```

## Rotas da API (100% atualizadas)

### Pedidos (`/api/pedidos`)

- `POST /api/pedidos`
  - Cria pedido e publica em `integrador.pedido.recebido`.
- `POST /api/pedidos/teste-carga?quantidade=1000`
  - Dispara carga sintética de pedidos e retorna métricas.
- `GET /api/pedidos/consumidos?limite=50`
  - Lista pedidos processados mantidos em memória.
- `GET /api/pedidos/h2/find-all`
  - Lista pedidos persistidos no H2.

Payload (`POST /api/pedidos`):

```json
{
  "numeroPedido": "PED-12345",
  "cliente": "Cliente A",
  "produto": "Notebook",
  "quantidade": 1,
  "valorTotal": "3500.00"
}
```

### Notas Fiscais (`/api/notas`)

- `POST /api/notas`
  - Cria nota fiscal e publica em `integrador.nota.recebido`.
- `GET /api/notas/consumidas?limite=50`
  - Lista notas fiscais processadas mantidas em memória.
- `GET /api/notas/h2/find-all`
  - Lista notas fiscais persistidas no H2.

Payload (`POST /api/notas`):

```json
{
  "numeroNota": "NF-12345",
  "cliente": "Cliente A",
  "produto": "Notebook",
  "quantidade": 1,
  "valorTotal": "3500.00"
}
```

### Reprocessamento (`/api/reprocessamento`)

- `GET /api/reprocessamento/falhas?tipo=PEDIDO|NOTA&status=PENDENTE_REPROCESSAMENTO&limite=100`
  - Lista falhas registradas para análise.
- `POST /api/reprocessamento/falhas/{id}/reprocessar`
  - Republica evento para tópico de entrada (`recebido`) e atualiza status.
- `POST /api/reprocessamento/falhas/{id}/descartar`
  - Marca falha como descartada (não reprocessar).

Status possíveis de falha:

- `PENDENTE_REPROCESSAMENTO`
- `REPROCESSADO`
- `ESGOTADO`
- `DESCARTADO`

## Teste rápido de fluxo Kafka

Entrar no container Kafka:

```bash
docker exec -it kafka bash
```

Produzir evento genérico:

```bash
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic integrador.evento.recebido
```

Consumir saída processada:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic integrador.evento.processado \
  --from-beginning
```

Consumir retry:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic integrador.evento.retry \
  --from-beginning
```

Consumir DLQ:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic integrador.evento.dlq \
  --from-beginning
```

## Configurações importantes

Arquivo: `src/main/resources/application.yaml`

- `server.port: 8082`
- `spring.kafka.listener.ack-mode: manual_immediate`
- `integrador.topico.*`: nomes dos tópicos
- `integrador.reprocessamento.max-tentativas: 5`
- `integrador.reprocessamento.intervalo-segundos: 60`
- `integrador.historico.falhas.limite: 2000`

## Observabilidade

- Métricas: `/actuator/metrics` e `/actuator/prometheus`
- Circuit breakers: `/actuator/circuitbreakers`
- Retries: `/actuator/retries`
- Logs centralizados: Loki (`LOKI_URL`)
- Dashboard: import automático em Grafana via `docker/grafana/provisioning`

## Produção (profile `prod`)

Variáveis principais:

- `SPRING_PROFILES_ACTIVE=prod`
- `KAFKA_BROKERS=<brokers>`
- `APP_ENV=prod`
- `LOKI_URL=http://loki:3100/loki/api/v1/push`

No profile `prod`, `show-details` do health é reduzido para não expor detalhes internos.

## Estrutura de diretórios (resumo)

```text
src/main/java/com/integracao/kafka/
  adapter/
    controller/
    dto/
  application/
    gateway/
    metrics/
    service/
    useCase/
  domain/
    entity/
    model/
  frameworkDrivers/
    config/
    kafka/
      consumer/
      producer/
```

## Próximos incrementos recomendados

- Persistir histórico de falhas em banco (hoje está em memória).
- Implementar Outbox Pattern no fallback do producer.
