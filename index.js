const express = require('express');
const rotasProduto = require('./rotas/rotasProduto');
require('dotenv').config();

const app = express();
app.use(express.json());
app.use(rotasProduto);

const porta = process.env.PORTA || 3000;

app.listen(porta, () => {
    console.log(`Servidor em execução: http://localhost:${porta}`);
});
