require('dotenv').config()
const amqp = require('amqplib')
const supabase = require('../config/database')

async function consume() {
    const exchange = 'estoque.eventos'
    const queue = 'fila.vitrine'

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

                if (evento.saldoAtual === 0) {
                    const { error } = await supabase
                        .from('produtos')
                        .update({ disponivel: false })
                        .eq('id', evento.produtoId)

                    if (error) {
                        console.error('Erro ao atualizar vitrine:', error.message)
                        return
                    }
                } else if (evento.tipo === 'ENTRADA' && evento.saldoAtual > 0) {
                    const { error } = await supabase
                        .from('produtos')
                        .update({ disponivel: true })
                        .eq('id', evento.produtoId)

                    if (error) {
                        console.error('Erro ao atualizar vitrine:', error.message)
                        return
                    }
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
