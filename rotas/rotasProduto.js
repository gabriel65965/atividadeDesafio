const express = require('express');
const produtoController = require('../controllers/produtoControl');

const router = express.Router();

router.get('/', produtoController.hello);
router.get('/produtos', produtoController.getProdutos);
router.get('/produtos/:id', produtoController.getById);
router.post('/produtos', produtoController.store);
router.delete('/produtos/:id', produtoController.deletaProduto);
router.post('/produtos/:id/movimentar', produtoController.movimentar);

module.exports = router;
