require('dotenv').config();
const amqp = require('amqplib');
const crypto = require('crypto');
const supabase = require('../config/database');

async function produce(evento) {
    const exchange = 'estoque.eventos';
    const message = JSON.stringify(evento);

    try {
        const connection = await amqp.connect(process.env.RABBITMQ_URL);
        const channel = await connection.createChannel();
        await channel.assertExchange(exchange, 'fanout', { durable: true });

        await channel.assertQueue('fila.auditoria', { durable: true });
        await channel.assertQueue('fila.compras', { durable: true });
        await channel.assertQueue('fila.vitrine', { durable: true });

        await channel.bindQueue('fila.auditoria', exchange, '');
        await channel.bindQueue('fila.compras', exchange, '');
        await channel.bindQueue('fila.vitrine', exchange, '');

        channel.publish(exchange, '', Buffer.from(message), {
            persistent: true
        });

        console.log(`[X] Mensagem enviada, ${message}`);
        setTimeout(() => {
            connection.close();
        }, 500);
    } catch (error) {
        console.log('Error no produtor!', error.message);
    }
}

const hello = (req, res) => {
    return res.status(200).json({ mensagem: "Deu bom" });
};

const getProdutos = async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('produtos')
            .select('*');
        if (error) return res.status(400).json({ mensagem: error.message });
        return res.status(200).json(data);
    } catch (error) {
        return res.status(500).json({ mensagem: error.message });
    }
};

const getById = async (req, res) => {
    try {
        const { id } = req.params;
        const { data, error } = await supabase
            .from('produtos')
            .select('*')
            .eq('id', id)
            .single();

        if (error || !data) {
            return res.status(404).json({ error: 'Produto não encontrado' });
        }

        return res.status(200).json(data);
    } catch (error) {
        return res.status(500).json({ error: error.message });
    }
};

const store = async (req, res) => {
    const { nome, preco, estoque, estoque_minimo } = req.body;

    if (!nome || preco == null || estoque == null) {
        return res.status(400).json({ error: "Os campos nome, preco e/ou estoque são obrigatórios" });
    }

    if (typeof preco != 'number' || preco < 0) {
        return res.status(400).json({ error: "O campo preco deve ser um número e positivo" });
    }

    try {
        const { data, error } = await supabase
            .from("produtos")
            .insert([{ nome, preco, estoque, estoque_minimo: estoque_minimo || 5, disponivel: estoque > 0 }])
            .select();
        if (error) return res.status(400).json({ error: error.message });
        return res.status(201).json(data);
    } catch (error) {
        return res.status(500).json({ error: error.message });
    }
};

const deletaProduto = async (req, res) => {
    const { id } = req.params;

    try {
        const { data, error } = await supabase
            .from('produtos')
            .delete()
            .eq('id', id)
            .select();
        if (error) return res.status(400).json({ error: "Produto não encontrado para remoção" });
        if (!data || data.length === 0) {
            return res.status(404).json({ error: "Não existe o produto." });
        }
        return res.json({ mensagem: "Produto removido com sucesso" });
    } catch (error) {
        return res.status(500).json({ error: error.message });
    }
};

const movimentar = async (req, res) => {
    const { id } = req.params;
    const { tipo, quantidade } = req.body;

    if (!tipo || !quantidade || quantidade <= 0) {
        return res.status(400).json({ error: "Informe o tipo (ENTRADA ou SAIDA) e uma quantidade válida" });
    }

    if (tipo !== 'ENTRADA' && tipo !== 'SAIDA') {
        return res.status(400).json({ error: "O tipo deve ser ENTRADA ou SAIDA" });
    }

    try {
        const { data: produto, error: erroBusca } = await supabase
            .from('produtos')
            .select('*')
            .eq('id', id)
            .single();

        if (erroBusca || !produto) {
            return res.status(404).json({ error: "Produto não encontrado" });
        }

        const saldoAnterior = Number(produto.estoque);

        if (tipo === 'SAIDA' && saldoAnterior < quantidade) {
            return res.status(400).json({ error: "Saldo insuficiente em estoque" });
        }

        const saldoAtual = tipo === 'ENTRADA'
            ? saldoAnterior + Number(quantidade)
            : saldoAnterior - Number(quantidade);

        const { error: erroUpdate } = await supabase
            .from('produtos')
            .update({ estoque: saldoAtual })
            .eq('id', id);

        if (erroUpdate) {
            return res.status(400).json({ error: erroUpdate.message });
        }

        const evento = {
            eventoId: crypto.randomUUID(),
            produtoId: Number(produto.id),
            nomeProduto: produto.nome,
            tipo: tipo,
            quantidadeMovimentada: Number(quantidade),
            saldoAnterior: saldoAnterior,
            saldoAtual: saldoAtual,
            estoqueMinimo: Number(produto.estoque_minimo),
            timestamp: new Date().toISOString()
        };

        produce(evento);

        return res.status(200).json(evento);
    } catch (error) {
        return res.status(500).json({ error: error.message });
    }
};

module.exports = {
    hello,
    getProdutos,
    getById,
    store,
    deletaProduto,
    movimentar
};
