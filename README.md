# Dashboard – Planejamento de Pedidos & Estoque

Dashboard interativo de atendimento de pedidos pendentes e controle de estoque,
gerado automaticamente a partir do Google Sheets (dados do OMIE).

## Acesso

O dashboard é publicado automaticamente em:
**https://thiagomas25.github.io/dashboard-estoque/**

## Atualização automática

O GitHub Actions roda todo dia útil às 7h (horário de Brasília) e republica o dashboard.
Para rodar manualmente: **Actions → Atualizar Dashboard → Run workflow**.

## Rodar localmente

```bash
pip install -r requirements.txt
python processar_dashboard.py   # usa arquivos locais (FONTE = "local")
```

Para usar o Google Sheets localmente:
1. Coloque `credenciais_sheets.json` na pasta do projeto
2. No script, mude `FONTE = "sheets"`
3. Rode `python processar_dashboard.py`
