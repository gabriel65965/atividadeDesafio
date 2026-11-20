require('dotenv').config()
const amqp = require('amqplib')
const supabase = require('../config/database')

async function consume() {
    const exchange = 'estoque.eventos'
    const queue = 'fila.auditoria'

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

                const { data: existente } = await supabase
                    .from('movimentacoes_estoque')
                    .select('id')
                    .eq('evento_id', evento.eventoId)
                    .maybeSingle()

                if (existente) {
                    console.log(`[V] Evento já processado: "${evento.eventoId}"`)
                    channel.ack(msg)
                    return
                }

                const { error } = await supabase
                    .from('movimentacoes_estoque')
                    .insert([{
                        evento_id: evento.eventoId,
                        produto_id: evento.produtoId,
                        tipo: evento.tipo,
                        quantidade: evento.quantidadeMovimentada,
                        saldo_anterior: evento.saldoAnterior,
                        saldo_atual: evento.saldoAtual,
                        criado_em: evento.timestamp
                    }])

                if (error) {
                    console.error('Erro ao salvar no Supabase:', error.message)
                    return
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
