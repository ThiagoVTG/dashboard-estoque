# Como conectar o Dashboard ao Google Sheets

## Pré-requisito: configurar a credencial (uma vez só)

### 1. Criar projeto no Google Cloud
1. Acesse https://console.cloud.google.com
2. Clique em "Novo Projeto" → nome: `dashboard-estoque` → Criar
3. No menu lateral: **APIs e serviços → Biblioteca**
4. Busque **"Google Sheets API"** → Ativar
5. Busque **"Google Drive API"** → Ativar

### 2. Criar conta de serviço e baixar credencial
1. Vá em **APIs e serviços → Credenciais**
2. Clique em **"Criar credenciais" → "Conta de serviço"**
3. Nome: `dashboard-estoque` → Criar e continuar → Concluído
4. Clique na conta criada → aba **"Chaves"**
5. **"Adicionar chave" → "Criar nova chave" → JSON → Criar**
6. Salve o arquivo baixado como:
   `C:\Users\thiag\Documents\Claude\BI estoque\credenciais_sheets.json`

### 3. Compartilhar a planilha com a conta de serviço
1. Abra o arquivo `credenciais_sheets.json` e copie o valor de `"client_email"`
   (algo como `dashboard-estoque@seu-projeto.iam.gserviceaccount.com`)
2. No Google Sheets, clique em **Compartilhar**
3. Cole o e-mail copiado → permissão **Leitor** é suficiente → Enviar

---

## Configurar o script

Abra `processar_dashboard.py` e edite **apenas** esta linha no topo:

```python
# Troque "local" por "sheets"
FONTE = "sheets"
```

Os IDs das planilhas e os gids das abas já estão pré-configurados:
- Pedidos:  `1q4dB7sZPzNBptN306gsi4fSH1mV3f5xpq9yAjbtcq38` / gid `1706031004`
- Estoque:  `1QBExoLdbwsd9NQRXKOibTrYx9vqOED5KEZQR_HvdAmk` / gid `1703010103`

Se algum dia você mudar de aba, basta copiar o novo `#gid=XXXXXX` da URL e atualizar
`GID_PEDIDOS` ou `GID_ESTOQUE` no script.

---

## Estrutura esperada das abas no Google Sheets

### Aba PEDIDOS PENDENTES
Deve ter as mesmas colunas do CSV atual:
- `Minha Empresa (Nome Fantasia)`, `Etapa`, `Data de Inclusão`, `Categoria`
- `Vendedor`, `Cliente (Nome Fantasia)`, `PEDIDO`, `Projeto`
- `Descrição Produtos`, `obs`, `QTDE PEDIDO`, `SALDO ATUAL`
- `envase pendente`, `FRASCOS`

### Aba ESTOQUE DIA
Deve ter as mesmas colunas do Excel atual:
- `Descrição do Produto`, `Marca`, `Modelo`, `Família de Produto`
- `SALDO ATUAL`, `Estoque Futuro`, `PENDENTE SAIDA`, `PENDENTE ENTRADA`
- `VENDAS (-)`, `QTDE PINTADOS`, `QTDE FRASCOS`

---

## Atualizar o dashboard

Após configurado, o fluxo de atualização é:

```
1. Exportar do OMIE → atualizar a planilha no Google Sheets
2. Rodar o script:
   python "C:\Users\thiag\Documents\Claude\BI estoque\processar_dashboard.py"
3. Abrir o arquivo gerado:
   C:\Users\thiag\Documents\Claude\BI estoque\DASHBOARD_PEDIDOS_ESTOQUE.html
```

Ou rode com um duplo-clique criando um `.bat`:

```bat
@echo off
python "C:\Users\thiag\Documents\Claude\BI estoque\processar_dashboard.py"
start "" "C:\Users\thiag\Documents\Claude\BI estoque\DASHBOARD_PEDIDOS_ESTOQUE.html"
pause
```

Salve como `ATUALIZAR_DASHBOARD.bat` na pasta do projeto.
