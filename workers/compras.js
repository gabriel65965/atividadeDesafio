require('dotenv').config()
const amqp = require('amqplib')
const supabase = require('../config/database')

async function consume() {
    const exchange = 'estoque.eventos'
    const queue = 'fila.compras'

    try {
        const connection = await amqp.connect(process.env.RABBITMQ_URL)
        const channel = await connection.createChannel()
        await channel.assertExchange(exchange, 'fanout', { durable: true })
        await channel.assertQueue(queue, { durable: true })
        await channel.bindQueue(queue, exchange, '')
        channel.prefetch(1)
        console.log(`[*] Aguardando mensagens chegarem na fila "${queue}"`)

        channel.consume(queue, async (msg) => {
            if (msg !== null) {
                const content = msg.content.toString()
                const evento = JSON.parse(content)
                console.log(`[V] Mensagem recebida com sucesso!`)

                if (evento.saldoAtual <= evento.estoqueMinimo) {
                    const { error } = await supabase
                        .from('alertas_reposicao')
                        .insert([{
                            produto_id: evento.produtoId,
                            saldo_atual: evento.saldoAtual,
                            estoque_minimo: evento.estoqueMinimo,
                            status: 'PENDENTE',
                            criado_em: evento.timestamp
                        }])

                    if (error) {
                        console.error('Erro ao salvar alerta no Supabase:', error.message)
                        return
                    }

                    console.log(JSON.stringify({
                        alerta: 'ESTOQUE_CRITICO',
                        produtoId: evento.produtoId,
                        nomeProduto: evento.nomeProduto,
                        saldoAtual: evento.saldoAtual,
                        estoqueMinimo: evento.estoqueMinimo,
                        status: 'PENDENTE',
                        timestamp: evento.timestamp
                    }))
                }

                console.log(`[V] Processamento concluido "${content}"`)
                channel.ack(msg)
            }
        }, {
            noAck: false,
        })
    } catch (error) {
        console.error('Erro no consumidor.', error.message)
    }
}

consume()
