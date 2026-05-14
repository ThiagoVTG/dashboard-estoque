"""
Dashboard de Planejamento de Atendimento de Pedidos e Estoque
Gerador de HTML interativo a partir das bases de dados

FONTES DE DADOS — edite a seção CONFIG abaixo para alternar entre
Google Sheets (privado) e arquivos locais (CSV/Excel).
"""

import os
import tempfile
import pandas as pd
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path

try:
    from rapidfuzz import fuzz, process as rfprocess
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False

try:
    import gspread
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False

# ════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO — edite aqui para trocar a fonte de dados
# ════════════════════════════════════════════════════════════════════════════

# Fonte ativa: "sheets" lê do Google Sheets | "local" lê dos arquivos abaixo
# Em CI (GitHub Actions) a env var DASHBOARD_FONTE sobrescreve este valor.
FONTE = os.environ.get("DASHBOARD_FONTE", "local")

# ── Google Sheets — duas planilhas separadas ─────────────────────────────────
SHEETS_ID_PEDIDOS  = "1q4dB7sZPzNBptN306gsi4fSH1mV3f5xpq9yAjbtcq38"
GID_PEDIDOS        = 1706031004

SHEETS_ID_ESTOQUE  = "1QBExoLdbwsd9NQRXKOibTrYx9vqOED5KEZQR_HvdAmk"
GID_ESTOQUE        = 1703010103

# Credencial: arquivo local OU conteúdo JSON via env var (GitHub Actions Secret)
CREDENCIAIS        = Path(__file__).parent / "credenciais_sheets.json"

# ── Arquivos locais (fallback / uso sem internet) ─────────────────────────────
PEDIDOS_CSV      = Path(r"C:\Users\thiag\Downloads\PEDIDOS PENDENTES - 14_05.csv")
ESTOQUE_XLSX     = Path(r"C:\Users\thiag\Downloads\CONTROLE DE INVENTARIO.xlsx")
ABA_ESTOQUE_XLSX = "ESTOQUE DIA"

# ── Saída ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
# Em CI, grava em docs/index.html (servido pelo GitHub Pages)
_ci = os.environ.get("CI", "")
OUTPUT_HTML = BASE_DIR / ("docs/index.html" if _ci else "DASHBOARD_PEDIDOS_ESTOQUE.html")

# ════════════════════════════════════════════════════════════════════════════


# ─── CARGA DO GOOGLE SHEETS ──────────────────────────────────────────────────
def _sheets_to_df(worksheet) -> pd.DataFrame:
    """Converte uma aba do gspread em DataFrame com header na linha 1."""
    rows = worksheet.get_all_values()
    if not rows:
        return pd.DataFrame()
    header = rows[0]
    data   = rows[1:]
    # Lidar com cabeçalhos duplicados (igual ao CSV)
    seen = {}
    clean_header = []
    for c in header:
        c = str(c).strip()
        if c in seen:
            seen[c] += 1
            clean_header.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            clean_header.append(c)
    return pd.DataFrame(data, columns=clean_header)


def _get_gspread_client():
    """Retorna cliente gspread usando arquivo local ou env var (CI)."""
    cred_json_str = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if cred_json_str:
        # Modo CI: escreve JSON da env var em arquivo temporário
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
        tmp.write(cred_json_str)
        tmp.flush()
        tmp.close()
        client = gspread.service_account(filename=tmp.name)
        Path(tmp.name).unlink(missing_ok=True)
        return client
    if CREDENCIAIS.exists():
        return gspread.service_account(filename=str(CREDENCIAIS))
    raise FileNotFoundError(
        f"Credenciais não encontradas: {CREDENCIAIS}\n"
        "Siga o guia COMO_CONECTAR_GOOGLE_SHEETS.md ou defina a env var GOOGLE_CREDENTIALS_JSON."
    )


def carregar_do_sheets():
    """Lê pedidos e estoque de duas planilhas Google Sheets privadas."""
    if not HAS_GSPREAD:
        raise ImportError("Instale o gspread: pip install gspread")

    gc = _get_gspread_client()

    # ── Pedidos ──────────────────────────────────────────────────────────────
    print(f"  Abrindo planilha de pedidos (gid={GID_PEDIDOS})...")
    sh_ped = gc.open_by_key(SHEETS_ID_PEDIDOS)
    ws_ped = sh_ped.get_worksheet_by_id(GID_PEDIDOS)
    df_ped = _sheets_to_df(ws_ped)
    print(f"  {len(df_ped)} linhas de pedidos carregadas da aba '{ws_ped.title}'")

    # ── Estoque ──────────────────────────────────────────────────────────────
    print(f"  Abrindo planilha de estoque (gid={GID_ESTOQUE})...")
    sh_est = gc.open_by_key(SHEETS_ID_ESTOQUE)
    ws_est = sh_est.get_worksheet_by_id(GID_ESTOQUE)
    df_est = _sheets_to_df(ws_est)
    print(f"  {len(df_est)} linhas de estoque carregadas da aba '{ws_est.title}'")

    return df_ped, df_est


# ─── UTILIDADES ──────────────────────────────────────────────────────────────
def normalizar(texto):
    """Remove acentos, lowercase, strip, colapsa espaços."""
    if not isinstance(texto, str):
        return ""
    txt = unicodedata.normalize("NFD", texto)
    txt = "".join(c for c in txt if unicodedata.category(c) != "Mn")
    txt = txt.lower().strip()
    txt = re.sub(r"\s+", " ", txt)
    txt = re.sub(r"[¿?]+", "", txt)
    return txt


def safe_float(v, default=0.0):
    if pd.isna(v):
        return default
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return default


def parse_qty(v):
    """Converte quantidade: remove trailing dot, trata separadores BR."""
    if pd.isna(v):
        return 0.0
    s = str(v).strip().rstrip(".")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def parse_date(v):
    if pd.isna(v):
        return ""
    s = str(v).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
        except Exception:
            pass
    return s


# ─── CARGA E LIMPEZA: PEDIDOS ─────────────────────────────────────────────────
def carregar_pedidos(df_raw=None):
    """Se df_raw for passado (vindo do Sheets), usa ele; caso contrário lê o CSV local."""
    print("Carregando pedidos pendentes...")
    if df_raw is not None:
        df = df_raw.copy().astype(str)
    else:
        df = pd.read_csv(PEDIDOS_CSV, encoding="utf-8", sep=",", dtype=str, on_bad_lines="skip")

    # Remover colunas completamente vazias
    df = df.dropna(axis=1, how="all")

    # Padronizar nomes de coluna
    df.columns = [str(c).strip() for c in df.columns]

    # Renomear colunas duplicadas e ambíguas
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for c in cols:
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    df.columns = new_cols

    # Mapeamento para nomes canônicos
    rename_map = {}
    col_lower = {c.lower(): c for c in df.columns}

    def find_col(*candidates):
        for cand in candidates:
            if cand in df.columns:
                return cand
            if cand.lower() in col_lower:
                return col_lower[cand.lower()]
        return None

    campos = {
        "empresa": find_col("Minha Empresa (Nome Fantasia)", "minha empresa (nome fantasia)"),
        "etapa": find_col("Etapa", "etapa"),
        "data": find_col("Data de Inclusão", "data de inclusão"),
        "categoria": find_col("Categoria", "categoria"),
        "vendedor": find_col("Vendedor", "vendedor"),
        "cliente": find_col("Cliente (Nome Fantasia)", "cliente (nome fantasia)"),
        "pedido": find_col("PEDIDO", "pedido"),
        "projeto": find_col("Projeto", "projeto"),
        "descricao": find_col("Descrição Produtos", "descrição produtos"),
        "obs": find_col("obs", "OBS"),
        "qtde_pedido": find_col("QTDE PEDIDO", "qtde pedido"),
        "saldo_atual": find_col("SALDO ATUAL", "saldo atual"),
        "dispo_pedido": find_col("DISPO. P/ PEDIDO", "dispo. p/ pedido"),
        "total_pedidos": find_col("TOTAL DE PEDIDOS PENDENTES", "total de pedidos pendentes"),
        "dispo_todos": find_col("DISPO.TODOS PEDIDOS", "dispo.todos pedidos"),
        "envase_pendente": find_col("envase pendente"),
        "frascos_col": find_col("FRASCOS", "frascos"),
        "falta_pedido": find_col("FALTA pedido", "falta pedido"),
        "envasar": find_col("envasar"),
        "produto_col": find_col("PRODUTO", "produto"),
        "pedidos_diferentes": find_col("PEDIDOS DIFERENTES", "pedidos diferentes"),
        "saldo_atual_2": find_col("SALDO ATUAL_1", "SALDO ATUAL_2"),
        "qtde_vendida": find_col("QTDE VENDIDA", "qtde vendida"),
        "dispo_todos_2": find_col("DISPO PARA TODO PEDIDOS", "dispo para todo pedidos"),
        "pedido_pendente_col": find_col("PEDIDO PENDENTE", "pedido pendente"),
        "fazer_pedido": find_col("FAZER PEDIDO", "fazer pedido"),
    }

    # Construir dataframe limpo
    rows = []
    for _, r in df.iterrows():
        def gc(key, default=""):
            col = campos.get(key)
            if col and col in r.index:
                v = r[col]
                return "" if pd.isna(v) else str(v).strip()
            return default

        qtde = parse_qty(gc("qtde_pedido"))
        if qtde == 0:
            continue  # linha de agrupamento, não pedido real

        saldo_raw = gc("saldo_atual")
        if saldo_raw in ("#VALUE!", "#N/A", ""):
            # Tentar saldo_atual_2
            saldo_raw = gc("saldo_atual_2")
        saldo = safe_float(saldo_raw)

        frascos_raw = gc("frascos_col")
        frascos = 0.0 if frascos_raw in ("#N/A", "#VALUE!", "") else safe_float(frascos_raw)

        total_ped_raw = gc("total_pedidos")
        total_ped = 0.0 if total_ped_raw in ("#VALUE!", "#N/A", "") else safe_float(total_ped_raw)

        descricao = gc("descricao")
        # Limpar caracteres corrompidos
        descricao = re.sub(r"[¿]+", "", descricao).strip()

        rows.append({
            "empresa": gc("empresa"),
            "etapa": gc("etapa"),
            "data": parse_date(gc("data")),
            "categoria": gc("categoria"),
            "vendedor": gc("vendedor"),
            "cliente": gc("cliente"),
            "pedido": gc("pedido"),
            "projeto": gc("projeto"),
            "descricao": descricao,
            "desc_norm": normalizar(descricao),
            "obs": gc("obs"),
            "qtde_pedido": qtde,
            "saldo_csv": saldo,
            "total_pedidos_pendentes": total_ped,
            "envase_pendente": safe_float(gc("envase_pendente")),
            "frascos_csv": frascos,
            "envasar_flag": gc("envasar"),
            "qtde_vendida": safe_float(gc("qtde_vendida")),
        })

    pedidos = pd.DataFrame(rows)
    print(f"  {len(pedidos)} linhas de pedido carregadas")
    return pedidos


# ─── CARGA E LIMPEZA: ESTOQUE ─────────────────────────────────────────────────
def carregar_estoque(df_raw=None):
    print("Carregando estoque...")
    if df_raw is not None:
        df = df_raw.copy().astype(str)
    else:
        df = pd.read_excel(ESTOQUE_XLSX, sheet_name=ABA_ESTOQUE_XLSX, dtype=str, header=0)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.dropna(axis=1, how="all")

    # Remover duplicatas
    df = df.drop_duplicates()

    def fc(*candidates):
        for cand in candidates:
            if cand in df.columns:
                return cand
        cl = {c.lower(): c for c in df.columns}
        for cand in candidates:
            if cand.lower() in cl:
                return cl[cand.lower()]
        return None

    campos_est = {
        "empresa": fc("Minha Empresa (Nome Fantasia)"),
        "codigo": fc("Código do Produto"),
        "descricao": fc("Descrição do Produto"),
        "marca": fc("Marca"),
        "modelo": fc("Modelo"),
        "familia": fc("Família de Produto"),
        "saldo_atual": fc("SALDO ATUAL"),
        "estoque_futuro": fc("Estoque Futuro"),
        "pendente_saida": fc("PENDENTE SAIDA"),
        "pendente_entrada": fc("PENDENTE ENTRADA"),
        "vendas": fc("VENDAS (-)"),
        "consumo": fc("CONSUMO PRODUÇÃO (-)"),
        "entradas_prod": fc("ENTRADAS PRODUÇÃO (+)"),
        "previsao_compras": fc("PREVISÃO COMPRAS (+)"),
        "estoque_minimo": fc("ESTOQUE MINIMO"),
        "necessidade_compra": fc("NECESSIDADE COMPRA"),
        "cmc": fc("CMC"),
        "qtde_pintados": fc("QTDE PINTADOS"),
        "qtde_frascos": fc("QTDE FRASCOS"),
        "de_para_pintado": fc("DE PARA PINTADO"),
        "frasco": fc("FRASCO"),
        "periodo": fc("Período"),
    }

    rows = []
    for _, r in df.iterrows():
        def ge(key, default=""):
            col = campos_est.get(key)
            if col and col in r.index:
                v = r[col]
                return "" if pd.isna(v) else str(v).strip()
            return default

        desc = ge("descricao")
        if not desc:
            continue

        # Filtrar apenas produto acabado (evitar matéria-prima)
        familia = ge("familia").lower()
        if familia and ("frasco" in familia or "materia" in familia or "componente" in familia
                        or "embalagem" in familia or "insumo" in familia):
            continue

        rows.append({
            "empresa": ge("empresa"),
            "codigo": ge("codigo"),
            "descricao": desc,
            "desc_norm": normalizar(desc),
            "marca": ge("marca"),
            "modelo": ge("modelo"),
            "familia": ge("familia"),
            "saldo_atual": safe_float(ge("saldo_atual")),
            "estoque_futuro": safe_float(ge("estoque_futuro")),
            "pendente_saida": safe_float(ge("pendente_saida")),
            "pendente_entrada": safe_float(ge("pendente_entrada")),
            "vendas_historico": abs(safe_float(ge("vendas"))),
            "estoque_minimo": safe_float(ge("estoque_minimo")),
            "necessidade_compra": safe_float(ge("necessidade_compra")),
            "cmc": safe_float(ge("cmc")),
            "qtde_pintados": safe_float(ge("qtde_pintados")),
            "qtde_frascos": safe_float(ge("qtde_frascos")),
            "periodo": ge("periodo"),
        })

    estoque = pd.DataFrame(rows)

    # Manter apenas linha mais recente por (empresa, codigo/descricao)
    if "periodo" in estoque.columns and estoque["periodo"].any():
        estoque = estoque.sort_values("periodo", ascending=False)
        estoque = estoque.drop_duplicates(subset=["empresa", "desc_norm"], keep="first")
    else:
        estoque = estoque.drop_duplicates(subset=["empresa", "desc_norm"], keep="first")

    print(f"  {len(estoque)} SKUs de estoque carregados")
    return estoque


# ─── JOIN PEDIDOS × ESTOQUE ───────────────────────────────────────────────────
def join_bases(pedidos, estoque):
    print("Fazendo join pedidos × estoque...")

    est_index = {row["desc_norm"]: row for _, row in estoque.iterrows()}
    est_keys = list(est_index.keys())

    sem_match = []
    matched_rows = []

    for idx, ped in pedidos.iterrows():
        key = ped["desc_norm"]
        est_row = None
        match_tipo = ""

        if key in est_index:
            est_row = est_index[key]
            match_tipo = "exato"
        elif HAS_RAPIDFUZZ and est_keys:
            result = rfprocess.extractOne(key, est_keys, scorer=fuzz.token_sort_ratio)
            if result and result[1] >= 85:
                est_row = est_index[result[0]]
                match_tipo = f"fuzzy({result[1]}%)"
            else:
                match_tipo = "sem_match"
        else:
            match_tipo = "sem_match"

        if est_row is not None:
            matched_rows.append({
                **ped.to_dict(),
                "est_saldo": est_row["saldo_atual"],
                "est_futuro": est_row.get("estoque_futuro", 0),
                "est_pendente_saida": est_row.get("pendente_saida", 0),
                "est_pendente_entrada": est_row.get("pendente_entrada", 0),
                "est_vendas": est_row.get("vendas_historico", 0),
                "est_minimo": est_row.get("estoque_minimo", 0),
                "est_pintados": est_row.get("qtde_pintados", 0),
                "est_frascos": est_row.get("qtde_frascos", 0),
                "est_marca": est_row.get("marca", ""),
                "est_modelo": est_row.get("modelo", ""),
                "est_familia": est_row.get("familia", ""),
                "est_cmc": est_row.get("cmc", 0),
                "match_tipo": match_tipo,
                "est_desc": est_row.get("descricao", ""),
            })
        else:
            matched_rows.append({
                **ped.to_dict(),
                "est_saldo": ped["saldo_csv"],  # usar dado do CSV
                "est_futuro": 0,
                "est_pendente_saida": 0,
                "est_pendente_entrada": ped.get("envase_pendente", 0),
                "est_vendas": ped.get("qtde_vendida", 0),
                "est_minimo": 0,
                "est_pintados": 0,
                "est_frascos": ped.get("frascos_csv", 0),
                "est_marca": "",
                "est_modelo": "",
                "est_familia": "",
                "est_cmc": 0,
                "match_tipo": match_tipo,
                "est_desc": "",
            })
            sem_match.append(ped["descricao"])

    df = pd.DataFrame(matched_rows)
    sem_match_uniq = list(set(sem_match))
    print(f"  {len(df) - len(sem_match)} com match | {len(sem_match)} sem match no estoque")
    return df, sem_match_uniq


# ─── CLASSIFICAÇÃO A→G ────────────────────────────────────────────────────────
STATUS_COLORS = {
    "A - Atende 100%": "#22c55e",
    "B - Atende parcial": "#f59e0b",
    "C - Não atende": "#ef4444",
    "D - Depende de envase": "#f97316",
    "E - Depende de pintura": "#8b5cf6",
    "F - Depende de compra": "#6b7280",
    "G - Decisão comercial": "#3b82f6",
}

STATUS_ACAO = {
    "A - Atende 100%": "Separar agora",
    "B - Atende parcial": "Separar parcialmente",
    "C - Não atende": "Aguardar produto",
    "D - Depende de envase": "Mandar para envase",
    "E - Depende de pintura": "Mandar para pintura",
    "F - Depende de compra": "Comprar / solicitar",
    "G - Decisão comercial": "Validar com comercial",
}


def classificar(df):
    print("Classificando pedidos...")

    # Calcular disputa por SKU (mesmo produto em múltiplos pedidos com saldo insuficiente)
    demanda_por_sku = df.groupby("desc_norm")["qtde_pedido"].sum()
    df["demanda_total_sku"] = df["desc_norm"].map(demanda_por_sku)
    df["disputa"] = (df["demanda_total_sku"] > df["est_saldo"]) & \
                    (df.groupby("desc_norm")["pedido"].transform("nunique") > 1)

    statuses = []
    qtde_poss = []
    qtde_falt = []
    prioridades = []

    for _, r in df.iterrows():
        saldo = max(r["est_saldo"], 0)
        qtde = r["qtde_pedido"]
        pintados = r["est_pintados"]
        frascos = r["est_frascos"]
        falta = max(qtde - saldo, 0)

        if saldo >= qtde:
            status = "A - Atende 100%"
            poss = qtde
        elif saldo > 0 and saldo < qtde:
            if r["disputa"]:
                status = "G - Decisão comercial"
            else:
                status = "B - Atende parcial"
            poss = saldo
        else:
            # Saldo zero ou negativo
            if pintados >= falta and falta > 0:
                status = "D - Depende de envase"
            elif frascos >= falta and falta > 0:
                status = "E - Depende de pintura"
            elif r["disputa"]:
                status = "G - Decisão comercial"
            else:
                status = "F - Depende de compra"
            poss = min(saldo, qtde) if saldo > 0 else 0

        # Prioridade: score (menor = mais urgente)
        score = 0
        try:
            d = datetime.strptime(r["data"], "%d/%m/%Y") if r["data"] else datetime(2099, 1, 1)
            dias = (datetime.today() - d).days
            score -= min(dias, 365)  # mais antigo = score menor
        except Exception:
            pass
        if status == "A - Atende 100%":
            score -= 100
        if r["disputa"]:
            score += 50

        pct = (poss / qtde * 100) if qtde > 0 else 0
        prioridade = 1 if status == "A - Atende 100%" else \
                     2 if status == "B - Atende parcial" else \
                     3 if status == "G - Decisão comercial" else \
                     4 if status in ("D - Depende de envase", "E - Depende de pintura") else 5

        statuses.append(status)
        qtde_poss.append(round(poss))
        qtde_falt.append(round(falta))
        prioridades.append(prioridade)

    df["status"] = statuses
    df["qtde_possivel"] = qtde_poss
    df["qtde_faltante"] = qtde_falt
    df["pct_atendimento"] = (df["qtde_possivel"] / df["qtde_pedido"] * 100).round(1)
    df["prioridade"] = prioridades
    df["acao"] = df["status"].map(STATUS_ACAO)
    df["status_cor"] = df["status"].map(STATUS_COLORS)

    return df


# ─── ALOCAÇÃO DE ESTOQUE (SKUs em disputa) ───────────────────────────────────
def calcular_alocacao(df):
    """Para SKUs com disputa, distribui saldo por ordem de data."""
    disputas = df[df["disputa"]].copy()
    alocacao = []

    for sku, grupo in disputas.groupby("desc_norm"):
        saldo_disp = grupo["est_saldo"].iloc[0]
        # Ordenar por data (mais antigo primeiro)
        try:
            grupo = grupo.copy()
            grupo["_dt"] = pd.to_datetime(grupo["data"], format="%d/%m/%Y", errors="coerce")
            grupo = grupo.sort_values("_dt")
        except Exception:
            pass

        saldo_restante = saldo_disp
        for _, row in grupo.iterrows():
            qtde = row["qtde_pedido"]
            alocado = min(saldo_restante, qtde)
            saldo_restante = max(saldo_restante - alocado, 0)
            alocacao.append({
                "produto": row["descricao"],
                "pedido": row["pedido"],
                "cliente": row["cliente"],
                "data": row["data"],
                "vendedor": row["vendedor"],
                "qtde_pedido": int(qtde),
                "saldo_disponivel": int(saldo_disp),
                "qtde_alocada": int(alocado),
                "qtde_faltante": int(max(qtde - alocado, 0)),
                "pct_atendimento": round(alocado / qtde * 100, 1) if qtde > 0 else 0,
                "criterio": "Data mais antiga primeiro",
                "risco": "Pedidos posteriores podem ser de clientes estratégicos" if alocado < qtde else "",
            })

    return pd.DataFrame(alocacao) if alocacao else pd.DataFrame()


# ─── GARGALOS POR PRODUTO ─────────────────────────────────────────────────────
def calcular_gargalos(df, estoque):
    print("Calculando gargalos por produto...")
    grp = df.groupby("desc_norm").agg(
        descricao=("descricao", "first"),
        empresa=("empresa", "first"),
        marca=("est_marca", "first"),
        modelo=("est_modelo", "first"),
        demanda_total=("qtde_pedido", "sum"),
        saldo_atual=("est_saldo", "first"),
        estoque_futuro=("est_futuro", "first"),
        vendas_hist=("est_vendas", "first"),
        qtde_pintados=("est_pintados", "first"),
        qtde_frascos=("est_frascos", "first"),
        envase_pendente=("est_pendente_entrada", "first"),
        n_pedidos=("pedido", "nunique"),
        n_clientes=("cliente", "nunique"),
    ).reset_index()

    grp["saldo_apos_pedidos"] = grp["saldo_atual"] - grp["demanda_total"]
    grp["falta_estimada"] = grp["saldo_apos_pedidos"].apply(lambda x: max(-x, 0))
    grp["media_mensal"] = (grp["vendas_hist"] / 12).round(1)
    grp["cobertura_meses"] = grp.apply(
        lambda r: round(r["saldo_atual"] / r["media_mensal"], 1) if r["media_mensal"] > 0 else 99, axis=1
    )

    def status_gargalo(r):
        if r["falta_estimada"] == 0:
            return "OK - Produto acabado"
        if r["qtde_pintados"] >= r["falta_estimada"] and r["falta_estimada"] > 0:
            return "Envase pendente"
        if r["qtde_frascos"] >= r["falta_estimada"] and r["falta_estimada"] > 0:
            return "Pintura pendente"
        if r["envase_pendente"] > 0:
            return "Entrada prevista"
        if r["saldo_apos_pedidos"] < 0:
            return "Compra necessária"
        return "Verificar dados"

    def acao_gargalo(r):
        s = r["status_gargalo"]
        if s == "OK - Produto acabado":
            return "Separar e expedir"
        if s == "Envase pendente":
            return f"Envasar {int(r['falta_estimada'])} unidades urgente"
        if s == "Pintura pendente":
            return f"Pintar {int(r['falta_estimada'])} frascos"
        if s == "Entrada prevista":
            return "Acompanhar chegada e reservar"
        if s == "Compra necessária":
            return f"Comprar {int(r['falta_estimada'])} unidades"
        return "Validar dados de estoque"

    grp["status_gargalo"] = grp.apply(status_gargalo, axis=1)
    grp["acao_sugerida"] = grp.apply(acao_gargalo, axis=1)

    return grp.sort_values("falta_estimada", ascending=False)


# ─── PLANO DE AÇÃO ────────────────────────────────────────────────────────────
def calcular_plano_acao(df):
    plano = {
        "separar_agora": df[df["status"] == "A - Atende 100%"][
            ["pedido", "cliente", "vendedor", "descricao", "qtde_pedido", "etapa"]
        ].to_dict("records"),
        "separar_parcial": df[df["status"] == "B - Atende parcial"][
            ["pedido", "cliente", "vendedor", "descricao", "qtde_pedido", "qtde_possivel", "qtde_faltante"]
        ].to_dict("records"),
        "envasar": df[df["status"] == "D - Depende de envase"][
            ["pedido", "cliente", "descricao", "qtde_pedido", "qtde_faltante", "est_pintados"]
        ].to_dict("records"),
        "pintar": df[df["status"] == "E - Depende de pintura"][
            ["pedido", "cliente", "descricao", "qtde_pedido", "qtde_faltante", "est_frascos"]
        ].to_dict("records"),
        "comprar": df[df["status"] == "F - Depende de compra"][
            ["pedido", "cliente", "descricao", "qtde_pedido", "qtde_faltante"]
        ].to_dict("records"),
        "validar_comercial": df[df["status"] == "G - Decisão comercial"][
            ["pedido", "cliente", "vendedor", "descricao", "qtde_pedido", "qtde_faltante", "demanda_total_sku"]
        ].to_dict("records"),
    }
    return plano


# ─── DIAGNÓSTICO ──────────────────────────────────────────────────────────────
def calcular_diagnostico(pedidos_raw, sem_match, df_merged):
    issues = []

    # Sem match no estoque
    for p in sem_match:
        issues.append({"tipo": "Produto sem match no estoque", "descricao": p, "acao": "Revisar nomenclatura"})

    # Saldo negativo no estoque
    neg = df_merged[df_merged["est_saldo"] < 0]
    for _, r in neg.iterrows():
        issues.append({
            "tipo": "Saldo negativo no estoque",
            "descricao": r["descricao"],
            "acao": f"Saldo: {r['est_saldo']} — verificar lançamentos"
        })

    # Demanda maior que saldo (sem disputa — para chamar atenção)
    falta = df_merged[(df_merged["qtde_faltante"] > 0) & (~df_merged["disputa"])]
    skus_falta = falta.groupby("desc_norm")["qtde_faltante"].sum()
    for sku, v in skus_falta.items():
        issues.append({
            "tipo": "Demanda excede estoque",
            "descricao": sku,
            "acao": f"Falta estimada: {int(v)} unidades"
        })

    return issues[:200]  # limitar para UI


# ─── KPIs EXECUTIVOS ──────────────────────────────────────────────────────────
def calcular_kpis(df):
    total_pedidos = len(df)
    total_skus = df["desc_norm"].nunique()
    total_itens = int(df["qtde_pedido"].sum())
    atende_100 = int((df["status"] == "A - Atende 100%").sum())
    atende_parcial = int((df["status"] == "B - Atende parcial").sum())
    nao_atende = int((df["status"].isin(["C - Não atende", "F - Depende de compra"])).sum())
    dep_envase = int((df["status"] == "D - Depende de envase").sum())
    dep_pintura = int((df["status"] == "E - Depende de pintura").sum())
    decisao_com = int((df["status"] == "G - Decisão comercial").sum())
    skus_ruptura = int((df.groupby("desc_norm")["est_saldo"].first() <= 0).sum())
    pct_atendimento = round(df["qtde_possivel"].sum() / df["qtde_pedido"].sum() * 100, 1) if total_itens > 0 else 0

    top_gargalos = df[df["qtde_faltante"] > 0].groupby("descricao")["qtde_faltante"].sum().nlargest(5).to_dict()
    top_clientes = df[df["qtde_faltante"] > 0].groupby("cliente")["qtde_faltante"].sum().nlargest(5).to_dict()
    por_etapa = df.groupby("etapa")["qtde_pedido"].sum().to_dict()
    por_empresa = df.groupby("empresa")["qtde_pedido"].sum().to_dict()
    por_vendedor = df.groupby("vendedor")["qtde_pedido"].sum().to_dict()
    por_status = df["status"].value_counts().to_dict()
    por_cliente = df.groupby("cliente")["qtde_pedido"].sum().nlargest(10).to_dict()

    return {
        "total_pedidos": total_pedidos,
        "total_skus": total_skus,
        "total_itens": total_itens,
        "atende_100": atende_100,
        "atende_parcial": atende_parcial,
        "nao_atende": nao_atende,
        "dep_envase": dep_envase,
        "dep_pintura": dep_pintura,
        "decisao_comercial": decisao_com,
        "skus_ruptura": skus_ruptura,
        "pct_atendimento": pct_atendimento,
        "top_gargalos": top_gargalos,
        "top_clientes": top_clientes,
        "por_etapa": por_etapa,
        "por_empresa": por_empresa,
        "por_vendedor": por_vendedor,
        "por_status": por_status,
        "por_cliente": por_cliente,
    }


# ─── SERIALIZAÇÃO SEGURA JSON ─────────────────────────────────────────────────
def to_json(obj):
    def default(o):
        if isinstance(o, float):
            if o != o:  # NaN
                return None
            return o
        return str(o)
    return json.dumps(obj, ensure_ascii=False, default=default)


# ─── GERAÇÃO HTML ─────────────────────────────────────────────────────────────
def gerar_html(kpis, df_merged, df_alocacao, df_gargalos, plano_acao, diagnostico):
    print("Gerando HTML do dashboard...")

    pedidos_json = to_json(df_merged[[
        "pedido", "cliente", "vendedor", "empresa", "data", "etapa",
        "descricao", "obs", "qtde_pedido", "est_saldo", "qtde_possivel",
        "qtde_faltante", "pct_atendimento", "status", "status_cor",
        "prioridade", "acao", "est_marca", "disputa", "match_tipo"
    ]].fillna("").to_dict("records"))

    alocacao_json = to_json(df_alocacao.fillna("").to_dict("records") if not df_alocacao.empty else [])

    gargalos_json = to_json(df_gargalos[[
        "descricao", "marca", "modelo", "demanda_total", "saldo_atual",
        "estoque_futuro", "saldo_apos_pedidos", "falta_estimada",
        "media_mensal", "cobertura_meses", "qtde_pintados", "qtde_frascos",
        "envase_pendente", "n_pedidos", "n_clientes", "status_gargalo", "acao_sugerida"
    ]].fillna("").to_dict("records"))

    plano_json = to_json(plano_acao)
    diag_json = to_json(diagnostico)
    kpis_json = to_json(kpis)
    colors_json = to_json(STATUS_COLORS)
    data_geracao = datetime.now().strftime("%d/%m/%Y %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Vitta Gold – Planejamento de Pedidos & Estoque</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700&family=Inter:wght@300;400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<style>
  :root{{
    --bg:#080704;
    --surface:#100f0a;
    --surface2:#171510;
    --border:#2a2418;
    --border-gold:#C9A84C44;
    --text:#f0ead8;
    --muted:#7a6e58;
    --gold:#C9A84C;
    --gold-light:#E8D4A0;
    --gold-dark:#8B6914;
    --gold-dim:#C9A84C22;
    --green:#4ade80;
    --yellow:#fbbf24;
    --red:#f87171;
    --orange:#fb923c;
    --purple:#a78bfa;
    --gray:#6b7280;
    --blue:#60a5fa;
  }}
  *{{box-sizing:border-box;margin:0;padding:0;}}
  body{{background:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif;font-size:14px;}}

  /* ── HEADER ── */
  header{{
    background:var(--surface);
    border-bottom:1px solid var(--border-gold);
    padding:0 28px;
    display:flex;align-items:center;justify-content:space-between;
    position:sticky;top:0;z-index:100;
    height:60px;
  }}
  .header-brand{{display:flex;align-items:center;gap:14px;}}
  .header-logo{{
    font-family:'Playfair Display',serif;
    font-size:20px;font-weight:700;
    background:linear-gradient(135deg,#E8D4A0,#C9A84C,#8B6914);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;
    background-clip:text;letter-spacing:1px;
  }}
  .header-divider{{width:1px;height:28px;background:var(--border-gold);}}
  .header-sub{{font-size:12px;color:var(--muted);font-weight:300;letter-spacing:.5px;}}
  header .header-date{{font-size:11px;color:var(--muted);}}

  /* ── NAV ── */
  nav{{display:flex;gap:4px;background:var(--surface);padding:8px 28px;border-bottom:1px solid var(--border);flex-wrap:wrap;}}
  nav button{{
    background:transparent;border:1px solid var(--border);color:var(--muted);
    padding:5px 14px;border-radius:4px;cursor:pointer;font-size:12px;
    font-family:'Inter',sans-serif;font-weight:500;letter-spacing:.3px;
    transition:all .2s;
  }}
  nav button:hover{{border-color:var(--gold-dark);color:var(--gold-light);}}
  nav button.active{{
    background:linear-gradient(135deg,#1a1508,#2a1f08);
    border-color:var(--gold);color:var(--gold);
    box-shadow:0 0 12px var(--gold-dim);
  }}

  .page{{display:none;padding:20px 28px;}}
  .page.active{{display:block;}}

  /* ── KPI CARDS ── */
  .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(165px,1fr));gap:12px;margin-bottom:22px;}}
  .kpi-card{{
    background:var(--surface);border:1px solid var(--border);
    border-radius:8px;padding:14px 16px;
    border-top:2px solid var(--border-gold);
    transition:border-color .2s;
  }}
  .kpi-card:hover{{border-top-color:var(--gold);}}
  .kpi-card .label{{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px;margin-bottom:8px;font-weight:500;}}
  .kpi-card .value{{font-size:28px;font-weight:600;font-family:'Playfair Display',serif;}}
  .kpi-card .sub{{font-size:11px;color:var(--muted);margin-top:4px;}}
  .kpi-gold .value{{color:var(--gold);}}
  .kpi-green .value{{color:var(--green);}}
  .kpi-yellow .value{{color:var(--yellow);}}
  .kpi-red .value{{color:var(--red);}}
  .kpi-blue .value{{color:var(--blue);}}
  .kpi-orange .value{{color:var(--orange);}}
  .kpi-purple .value{{color:var(--purple);}}

  /* ── CHARTS ── */
  .charts-row{{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px;margin-bottom:20px;}}
  .chart-box{{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:18px;}}
  .chart-box h3{{font-size:11px;color:var(--muted);margin-bottom:14px;text-transform:uppercase;letter-spacing:.8px;font-weight:500;}}
  .chart-box canvas{{max-height:220px;}}

  /* ── TABLES ── */
  .table-container{{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;overflow:auto;}}
  .section-title{{
    font-family:'Playfair Display',serif;
    font-size:18px;font-weight:600;margin-bottom:16px;
    color:var(--gold-light);letter-spacing:.5px;
  }}

  /* ── FILTER BAR ── */
  .filter-bar{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px;align-items:center;}}
  .filter-bar select,.filter-bar input,.search-box{{
    background:var(--surface2);border:1px solid var(--border);
    color:var(--text);border-radius:4px;padding:6px 10px;font-size:13px;
    font-family:'Inter',sans-serif;outline:none;
    transition:border-color .2s;
  }}
  .filter-bar select:focus,.search-box:focus{{border-color:var(--gold-dark);}}
  .search-box{{width:260px;}}
  .list-count{{font-size:12px;color:var(--muted);margin-left:4px;}}

  /* ── GROUPED TABLE ── */
  .grouped-table{{width:100%;border-collapse:collapse;font-size:13px;}}
  .grouped-table thead th{{
    background:var(--bg);color:var(--muted);font-weight:500;
    font-size:10px;text-transform:uppercase;letter-spacing:.6px;
    padding:8px 10px;border-bottom:1px solid var(--border);
    white-space:nowrap;position:sticky;top:0;z-index:10;
  }}
  .grouped-table tbody td{{padding:7px 10px;border-bottom:1px solid var(--border);vertical-align:middle;}}
  .grouped-table tbody tr:hover td{{background:rgba(201,168,76,.04);}}

  /* ── GROUP HEADER ROW — separador dourado entre pedidos/produtos ── */
  .grouped-table tr.grp-hdr td{{
    background:linear-gradient(90deg,#1a1508,#110e05);
    border-top:2px solid var(--gold-dark);
    border-bottom:1px solid var(--border-gold);
    font-weight:600;font-size:12px;color:var(--gold-light);
    padding:10px 10px;letter-spacing:.2px;
  }}
  .grouped-table tr.grp-hdr:first-of-type td{{border-top:none;}}

  /* ── BADGE ── */
  .badge{{display:inline-block;padding:2px 8px;border-radius:3px;font-size:11px;font-weight:600;white-space:nowrap;letter-spacing:.2px;}}

  /* ── PROGRESS BAR ── */
  .pbar-wrap{{background:var(--border);border-radius:2px;height:6px;width:80px;display:inline-block;vertical-align:middle;}}
  .pbar{{height:6px;border-radius:2px;}}

  /* ── PLANO DE AÇÃO ── */
  .plano-section{{margin-bottom:16px;}}
  .plano-section h3{{font-size:12px;font-weight:600;padding:9px 14px;border-radius:4px 4px 0 0;letter-spacing:.3px;}}
  .plano-table{{width:100%;border-collapse:collapse;}}
  .plano-table td,.plano-table th{{padding:7px 10px;border-bottom:1px solid var(--border);font-size:13px;}}
  .plano-table th{{font-size:10px;color:var(--muted);font-weight:500;text-transform:uppercase;letter-spacing:.5px;}}

  /* ── DIAG ── */
  .issue-card{{background:var(--surface);border-left:2px solid var(--red);border-radius:0 4px 4px 0;padding:8px 12px;margin-bottom:6px;font-size:13px;}}
  .issue-card .tipo{{font-size:10px;color:var(--red);font-weight:600;margin-bottom:2px;text-transform:uppercase;letter-spacing:.4px;}}

  /* ── GOLD DIVIDER LINE ── */
  .gold-line{{height:1px;background:linear-gradient(90deg,transparent,var(--gold),transparent);margin:2px 0 18px;opacity:.4;}}
</style>
</head>
<body>
<header>
  <div class="header-brand">
    <span class="header-logo">VITTA GOLD</span>
    <div class="header-divider"></div>
    <span class="header-sub">Planejamento de Pedidos &amp; Estoque</span>
  </div>
  <span class="header-date">Gerado em {data_geracao}</span>
</header>
<nav>
  <button class="active" onclick="showPage('executivo',this)">1 · Visão Executiva</button>
  <button onclick="showPage('pedidos',this)">2 · Atendimento de Pedidos</button>
  <button onclick="showPage('alocacao',this)">3 · Alocação de Estoque</button>
  <button onclick="showPage('gargalos',this)">4 · Gargalos por Produto</button>
  <button onclick="showPage('plano',this)">5 · Plano de Ação</button>
  <button onclick="showPage('diagnostico',this)">6 · Diagnóstico de Dados</button>
</nav>

<!-- PAGE 1: EXECUTIVO -->
<div id="page-executivo" class="page active">
  <div class="section-title">Visão Executiva</div>
  <div class="kpi-grid" id="kpi-grid"></div>
  <div class="charts-row">
    <div class="chart-box"><h3>Status dos Pedidos</h3><canvas id="chart-status"></canvas></div>
    <div class="chart-box"><h3>Volume por Etapa</h3><canvas id="chart-etapa"></canvas></div>
    <div class="chart-box"><h3>Top 10 Clientes (Qtde Pendente)</h3><canvas id="chart-clientes"></canvas></div>
    <div class="chart-box"><h3>Volume por Empresa/Marca</h3><canvas id="chart-empresa"></canvas></div>
    <div class="chart-box"><h3>Top Produtos Gargalo (Falta)</h3><canvas id="chart-gargalos"></canvas></div>
    <div class="chart-box"><h3>Volume por Vendedor</h3><canvas id="chart-vendedor"></canvas></div>
  </div>
</div>

<!-- PAGE 2: PEDIDOS -->
<div id="page-pedidos" class="page">
  <div class="section-title">Atendimento de Pedidos</div>
  <div class="filter-bar">
    <input class="search-box" id="search-pedidos" placeholder="Buscar pedido, cliente ou produto..." oninput="filterPedidos()">
    <select id="filter-status" onchange="filterPedidos()">
      <option value="">Todos os status</option>
      <option>A - Atende 100%</option>
      <option>B - Atende parcial</option>
      <option>C - Não atende</option>
      <option>D - Depende de envase</option>
      <option>E - Depende de pintura</option>
      <option>F - Depende de compra</option>
      <option>G - Decisão comercial</option>
    </select>
    <select id="filter-empresa" onchange="filterPedidos()"><option value="">Todas as empresas</option></select>
    <select id="filter-vendedor" onchange="filterPedidos()"><option value="">Todos os vendedores</option></select>
    <select id="filter-prioridade" onchange="filterPedidos()">
      <option value="">Todas as prioridades</option>
      <option value="1">P1 – Separar agora</option>
      <option value="2">P2 – Parcial</option>
      <option value="3">P3 – Decisão comercial</option>
      <option value="4">P4 – Produção</option>
      <option value="5">P5 – Comprar</option>
    </select>
    <span class="list-count" id="count-pedidos"></span>
  </div>
  <div class="table-container" style="overflow:auto;max-height:80vh;">
    <table class="grouped-table">
      <thead><tr>
        <th style="width:30px">#</th><th>Produto</th>
        <th style="text-align:right">Qtde Pedida</th><th style="text-align:right">Saldo</th>
        <th style="text-align:right">Possível</th><th style="text-align:right">Faltante</th>
        <th>% Atend.</th><th>Status</th><th>Ação</th>
      </tr></thead>
      <tbody id="tbody-pedidos"></tbody>
    </table>
  </div>
</div>

<!-- PAGE 3: ALOCAÇÃO -->
<div id="page-alocacao" class="page">
  <div class="section-title">Alocação de Estoque – SKUs em Disputa</div>
  <p style="color:var(--muted);margin-bottom:14px;font-size:13px;">
    Quando mais de um pedido disputa o mesmo SKU com saldo insuficiente, o sistema propõe alocação por data (mais antigo primeiro).
    Revise e ajuste conforme critérios comerciais (cliente estratégico, pedido pago, etc.).
  </p>
  <div class="filter-bar">
    <input class="search-box" id="search-alocacao" placeholder="Buscar produto ou cliente..." oninput="filterAlocacao()">
    <span class="list-count" id="count-alocacao"></span>
  </div>
  <div class="table-container" style="overflow:auto;max-height:80vh;">
    <table class="grouped-table">
      <thead><tr>
        <th style="width:30px">#</th><th>Pedido</th><th>Cliente</th><th>Vendedor</th><th>Data</th>
        <th style="text-align:right">Qtde Pedida</th><th style="text-align:right">Saldo Disp.</th>
        <th style="text-align:right">Alocado</th><th style="text-align:right">Faltante</th>
        <th>% Atend.</th><th>Risco</th>
      </tr></thead>
      <tbody id="tbody-alocacao"></tbody>
    </table>
  </div>
</div>

<!-- PAGE 4: GARGALOS -->
<div id="page-gargalos" class="page">
  <div class="section-title">Gargalos por Produto / SKU</div>
  <div class="filter-bar">
    <input class="search-box" id="search-gargalos" placeholder="Buscar produto ou marca..." oninput="filterGargalos()">
    <span class="list-count" id="count-gargalos"></span>
  </div>
  <div class="table-container" style="overflow:auto;max-height:80vh;">
    <table class="grouped-table">
      <thead><tr>
        <th>Produto</th><th>Marca</th><th style="text-align:right">Demanda Total</th>
        <th style="text-align:right">Saldo Atual</th><th style="text-align:right">Est. Futuro</th>
        <th style="text-align:right">Saldo Pós Pedidos</th><th style="text-align:right">Falta Est.</th>
        <th style="text-align:right">Média Mensal</th><th style="text-align:right">Cobertura (m)</th>
        <th style="text-align:right">Pintados</th><th style="text-align:right">Frascos</th>
        <th style="text-align:right">Env.Pend.</th><th style="text-align:center">Ped.</th>
        <th style="text-align:center">Cli.</th><th>Status Gargalo</th><th>Ação Sugerida</th>
      </tr></thead>
      <tbody id="tbody-gargalos"></tbody>
    </table>
  </div>
</div>

<!-- PAGE 5: PLANO DE AÇÃO -->
<div id="page-plano" class="page">
  <div class="section-title">Plano de Ação Operacional</div>
  <div id="plano-container"></div>
</div>

<!-- PAGE 6: DIAGNÓSTICO -->
<div id="page-diagnostico" class="page">
  <div class="section-title">Diagnóstico de Dados</div>
  <p style="color:var(--muted);margin-bottom:14px;font-size:13px;">
    Problemas identificados na qualidade dos dados. Corrija antes da próxima atualização.
  </p>
  <div id="diag-container"></div>
</div>

<script>
// ─── DATA ────────────────────────────────────────────────────────────────────
const KPIS = {kpis_json};
const PEDIDOS = {pedidos_json};
const ALOCACAO = {alocacao_json};
const GARGALOS = {gargalos_json};
const PLANO = {plano_json};
const DIAGNOSTICO = {diag_json};
const STATUS_COLORS = {colors_json};

// ─── NAVIGATION ──────────────────────────────────────────────────────────────
let tablesInit = {{}};
function showPage(id, btn) {{
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
  document.getElementById('page-' + id).classList.add('active');
  btn.classList.add('active');
  if (!tablesInit[id]) {{ initPage(id); tablesInit[id] = true; }}
}}
function initPage(id) {{
  if (id === 'pedidos') initPedidos();
  if (id === 'alocacao') initAlocacao();
  if (id === 'gargalos') initGargalos();
  if (id === 'plano') initPlano();
  if (id === 'diagnostico') initDiagnostico();
}}

// ─── UTILS ───────────────────────────────────────────────────────────────────
function badge(text, color) {{
  return `<span class="badge" style="background:${{color}}22;color:${{color}};border:1px solid ${{color}}44">${{text}}</span>`;
}}
function pbar(pct) {{
  const c = pct >= 100 ? '#22c55e' : pct >= 50 ? '#f59e0b' : '#ef4444';
  return `<div class="pbar-wrap"><div class="pbar" style="width:${{Math.min(pct,100)}}%;background:${{c}}"></div></div> ${{pct}}%`;
}}
function num(v) {{ return (v == null || isNaN(v)) ? '-' : Number(v).toLocaleString('pt-BR'); }}

// ─── PAGE 1: KPIs + CHARTS ───────────────────────────────────────────────────
(function initExecutivo() {{
  const k = KPIS;
  const cards = [
    {{ label:'Total de Pedidos', value: num(k.total_pedidos), sub: num(k.total_skus)+' SKUs distintos', cls:'kpi-blue' }},
    {{ label:'Itens Totais Pendentes', value: num(k.total_itens), sub:'unidades', cls:'kpi-blue' }},
    {{ label:'% Atendimento Possível', value: k.pct_atendimento+'%', sub:'com estoque atual', cls: k.pct_atendimento>=70?'kpi-green':k.pct_atendimento>=40?'kpi-yellow':'kpi-red' }},
    {{ label:'Atende 100%', value: num(k.atende_100), sub:'pedidos prontos', cls:'kpi-green' }},
    {{ label:'Atende Parcial', value: num(k.atende_parcial), sub:'pedidos parciais', cls:'kpi-yellow' }},
    {{ label:'Não Atende', value: num(k.nao_atende), sub:'sem estoque', cls:'kpi-red' }},
    {{ label:'Depende de Envase', value: num(k.dep_envase), sub:'frascos disponíveis', cls:'kpi-orange' }},
    {{ label:'Depende de Pintura', value: num(k.dep_pintura), sub:'frascos não pintados', cls:'kpi-purple' }},
    {{ label:'Decisão Comercial', value: num(k.decisao_comercial), sub:'disputa de estoque', cls:'kpi-blue' }},
    {{ label:'SKUs em Ruptura', value: num(k.skus_ruptura), sub:'saldo ≤ 0', cls:'kpi-red' }},
  ];
  const grid = document.getElementById('kpi-grid');
  cards.forEach(c => {{
    grid.innerHTML += `<div class="kpi-card ${{c.cls}}"><div class="label">${{c.label}}</div><div class="value">${{c.value}}</div><div class="sub">${{c.sub}}</div></div>`;
  }});

  function mkChart(id, type, labels, data, colors, opts={{}}) {{
    const ctx = document.getElementById(id);
    if (!ctx) return;
    new Chart(ctx, {{
      type, data: {{ labels, datasets:[{{ data, backgroundColor: colors, borderColor: type==='bar'?colors:undefined, borderWidth: type==='bar'?0:2 }}] }},
      options: {{ responsive:true, plugins:{{ legend:{{ display: type!=='bar', labels:{{ color:'#7a6e58', font:{{size:11}} }} }}, tooltip:{{ callbacks:{{ label: ctx => ' '+ctx.formattedValue }} }} }}, scales: type==='bar'?{{ x:{{ ticks:{{ color:'#7a6e58',font:{{size:11}} }}, grid:{{ color:'#1a1508' }} }}, y:{{ ticks:{{ color:'#7a6e58',font:{{size:11}} }}, grid:{{ color:'#2a2418' }} }} }}:undefined, ...opts }}
    }});
  }}

  // Status donut
  const statusKeys = Object.keys(k.por_status);
  const statusVals = statusKeys.map(s => k.por_status[s]);
  const statusCols = statusKeys.map(s => STATUS_COLORS[s] || '#6b7280');
  mkChart('chart-status', 'doughnut', statusKeys.map(s=>s.split(' - ')[1]||s), statusVals, statusCols);

  // Etapa
  const etKeys = Object.keys(k.por_etapa).sort((a,b)=>k.por_etapa[b]-k.por_etapa[a]);
  mkChart('chart-etapa','bar',etKeys,etKeys.map(e=>k.por_etapa[e]),Array(etKeys.length).fill('#C9A84C'));

  // Clientes
  const clKeys = Object.keys(k.por_cliente).slice(0,10);
  mkChart('chart-clientes','bar',clKeys,clKeys.map(c=>k.por_cliente[c]),Array(clKeys.length).fill('#C9A84C'));

  // Empresa
  const empKeys = Object.keys(k.por_empresa);
  mkChart('chart-empresa','doughnut',empKeys,empKeys.map(e=>k.por_empresa[e]),['#C9A84C','#E8D4A0','#8B6914','#f0ead8']);

  // Gargalos
  const gKeys = Object.keys(k.top_gargalos).slice(0,5);
  mkChart('chart-gargalos','bar',gKeys.map(g=>g.length>30?g.slice(0,30)+'…':g),gKeys.map(g=>k.top_gargalos[g]),Array(gKeys.length).fill('#f87171'));

  // Vendedor
  const vKeys = Object.keys(k.por_vendedor).sort((a,b)=>k.por_vendedor[b]-k.por_vendedor[a]);
  mkChart('chart-vendedor','bar',vKeys,vKeys.map(v=>k.por_vendedor[v]),Array(vKeys.length).fill('#C9A84C'));
}})();

// ─── PAGE 2: PEDIDOS — lista agrupada por pedido ─────────────────────────────
const allPedidos = PEDIDOS;

function filterPedidos() {{
  const q   = (document.getElementById('search-pedidos')?.value||'').toLowerCase();
  const st  = document.getElementById('filter-status').value;
  const emp = document.getElementById('filter-empresa').value;
  const vnd = document.getElementById('filter-vendedor').value;
  const pri = document.getElementById('filter-prioridade').value;
  const filtered = allPedidos.filter(r =>
    (!q  || [r.pedido,r.cliente,r.descricao,r.vendedor].join(' ').toLowerCase().includes(q)) &&
    (!st  || r.status === st) &&
    (!emp || r.empresa === emp) &&
    (!vnd || r.vendedor === vnd) &&
    (!pri || String(r.prioridade) === pri)
  );
  renderPedidosGrouped(filtered);
}}

function renderPedidosGrouped(data) {{
  // agrupar por pedido preservando ordem de prioridade → data → pedido
  const ordered = [...data].sort((a,b) => {{
    if (a.prioridade !== b.prioridade) return a.prioridade - b.prioridade;
    return (a.pedido||'').localeCompare(b.pedido||'');
  }});
  const groups = {{}};
  const gOrder = [];
  ordered.forEach(r => {{
    const k = r.pedido || '(sem pedido)';
    if (!groups[k]) {{ groups[k] = []; gOrder.push(k); }}
    groups[k].push(r);
  }});

  let html = '';
  let totalLinhas = 0;
  gOrder.forEach(pedKey => {{
    const rows = groups[pedKey];
    const first = rows[0];
    const totalQtde = rows.reduce((s,r)=>s+(r.qtde_pedido||0),0);
    const totalPoss  = rows.reduce((s,r)=>s+(r.qtde_possivel||0),0);
    const totalFalt  = rows.reduce((s,r)=>s+(r.qtde_faltante||0),0);
    const pctGeral   = totalQtde>0?Math.round(totalPoss/totalQtde*100):0;
    const pbarC      = pctGeral>=100?'#22c55e':pctGeral>=50?'#f59e0b':'#ef4444';
    // linha de cabeçalho do pedido
    html += `<tr class="grp-hdr"><td colspan="9">
      <span style="color:var(--gold);font-size:12px;margin-right:10px">${{pedKey}}</span>
      <span style="margin-right:16px">${{first.cliente||''}}</span>
      <span style="color:var(--muted);font-weight:400;margin-right:16px">${{first.vendedor||''}}</span>
      <span style="color:var(--muted);font-weight:400;margin-right:16px">${{first.data||''}}</span>
      <span style="color:var(--muted);font-weight:400;margin-right:16px">Etapa: ${{first.etapa||'-'}}</span>
      <span style="margin-right:16px">${{rows.length}} produto(s)</span>
      <span style="color:${{pbarC}};margin-right:6px">${{pctGeral}}% atendido</span>
      <span style="font-size:11px;color:var(--muted)">Total pedido: ${{num(totalQtde)}} | Possível: ${{num(totalPoss)}} | Falta: ${{num(totalFalt)}}</span>
    </td></tr>`;
    // linhas de produto
    rows.forEach((r, i) => {{
      html += `<tr>
        <td style="text-align:center;color:var(--muted);font-size:11px">${{i+1}}</td>
        <td title="${{r.descricao}}">${{r.descricao||'-'}}</td>
        <td style="text-align:right">${{num(r.qtde_pedido)}}</td>
        <td style="text-align:right;color:${{r.est_saldo<0?'#ef4444':'inherit'}}">${{num(r.est_saldo)}}</td>
        <td style="text-align:right;color:#22c55e">${{num(r.qtde_possivel)}}</td>
        <td style="text-align:right;color:${{r.qtde_faltante>0?'#ef4444':'#22c55e'}};font-weight:${{r.qtde_faltante>0?700:400}}">${{num(r.qtde_faltante)}}</td>
        <td>${{pbar(r.pct_atendimento||0)}}</td>
        <td>${{badge(r.status?.split(' - ')[1]||r.status, r.status_cor||'#6b7280')}}</td>
        <td style="font-size:12px;color:var(--muted)">${{r.acao||'-'}}</td>
      </tr>`;
      totalLinhas++;
    }});
  }});

  document.getElementById('tbody-pedidos').innerHTML = html;
  const c = document.getElementById('count-pedidos');
  if(c) c.textContent = totalLinhas + ' produto(s) em ' + gOrder.length + ' pedido(s)';
}}

function initPedidos() {{
  const empresas = [...new Set(allPedidos.map(r=>r.empresa).filter(Boolean))];
  const vendedores = [...new Set(allPedidos.map(r=>r.vendedor).filter(Boolean))];
  document.getElementById('filter-empresa').innerHTML += empresas.map(e=>`<option>${{e}}</option>`).join('');
  document.getElementById('filter-vendedor').innerHTML += vendedores.map(v=>`<option>${{v}}</option>`).join('');
  renderPedidosGrouped(allPedidos);
}}

// ─── PAGE 3: ALOCAÇÃO — lista agrupada por produto ───────────────────────────
function filterAlocacao() {{
  const q = (document.getElementById('search-alocacao')?.value||'').toLowerCase();
  const filtered = (ALOCACAO||[]).filter(r =>
    !q || [r.produto,r.cliente,r.pedido].join(' ').toLowerCase().includes(q)
  );
  renderAlocacaoGrouped(filtered);
}}

function renderAlocacaoGrouped(data) {{
  if (!data || data.length === 0) {{
    document.getElementById('tbody-alocacao').innerHTML =
      '<tr><td colspan="11" style="text-align:center;color:var(--muted);padding:20px">Nenhum SKU em disputa identificado.</td></tr>';
    return;
  }}
  const groups = {{}};
  const gOrder = [];
  data.forEach(r => {{
    const k = r.produto || '(sem produto)';
    if (!groups[k]) {{ groups[k] = []; gOrder.push(k); }}
    groups[k].push(r);
  }});
  let html = '';
  let count = 0;
  gOrder.forEach(prod => {{
    const rows = groups[prod];
    const saldo = rows[0].saldo_disponivel;
    const totalDemanda = rows.reduce((s,r)=>s+(r.qtde_pedido||0),0);
    html += `<tr class="grp-hdr"><td colspan="11">
      <span style="color:var(--gold);font-size:12px;margin-right:10px">${{prod}}</span>
      <span style="color:var(--muted);font-weight:400;margin-right:16px">Saldo disponível: <strong style="color:#f1f5f9">${{num(saldo)}}</strong></span>
      <span style="color:var(--muted);font-weight:400;margin-right:16px">Demanda total: <strong style="color:${{totalDemanda>saldo?'#ef4444':'#22c55e'}}">${{num(totalDemanda)}}</strong></span>
      <span style="color:var(--muted);font-weight:400">${{rows.length}} pedido(s) disputando este SKU</span>
    </td></tr>`;
    rows.forEach((r, i) => {{
      html += `<tr>
        <td style="text-align:center;color:var(--muted);font-size:11px">${{i+1}}</td>
        <td>${{r.pedido||'-'}}</td><td>${{r.cliente||'-'}}</td>
        <td>${{r.vendedor||'-'}}</td><td>${{r.data||'-'}}</td>
        <td style="text-align:right">${{num(r.qtde_pedido)}}</td>
        <td style="text-align:right">${{num(r.saldo_disponivel)}}</td>
        <td style="text-align:right;color:#22c55e;font-weight:${{r.qtde_alocada>0?700:400}}">${{num(r.qtde_alocada)}}</td>
        <td style="text-align:right;color:${{r.qtde_faltante>0?'#ef4444':'#22c55e'}};font-weight:${{r.qtde_faltante>0?700:400}}">${{num(r.qtde_faltante)}}</td>
        <td>${{pbar(r.pct_atendimento||0)}}</td>
        <td style="font-size:12px;color:${{r.risco?'#f59e0b':'var(--muted)'}}">${{r.risco||'–'}}</td>
      </tr>`;
      count++;
    }});
  }});
  document.getElementById('tbody-alocacao').innerHTML = html;
  const c = document.getElementById('count-alocacao');
  if(c) c.textContent = count + ' linha(s) | ' + gOrder.length + ' SKU(s) em disputa';
}}

function initAlocacao() {{
  renderAlocacaoGrouped(ALOCACAO||[]);
}}

// ─── PAGE 4: GARGALOS — lista agrupada por status do gargalo ─────────────────
const GARGALO_COLORS = {{
  'OK - Produto acabado':'#22c55e','Envase pendente':'#f97316',
  'Pintura pendente':'#8b5cf6','Entrada prevista':'#3b82f6',
  'Compra necessária':'#ef4444','Verificar dados':'#6b7280'
}};
const GARGALO_ORDER = ['Compra necessária','Envase pendente','Pintura pendente','Entrada prevista','Verificar dados','OK - Produto acabado'];

function filterGargalos() {{
  const q = (document.getElementById('search-gargalos')?.value||'').toLowerCase();
  const filtered = GARGALOS.filter(r =>
    !q || [r.descricao,r.marca,r.status_gargalo].join(' ').toLowerCase().includes(q)
  );
  renderGargalosGrouped(filtered);
}}

function renderGargalosGrouped(data) {{
  const groups = {{}};
  GARGALO_ORDER.forEach(s => {{ groups[s] = []; }});
  data.forEach(r => {{
    const k = r.status_gargalo || 'Verificar dados';
    if (!groups[k]) groups[k] = [];
    groups[k].push(r);
  }});
  let html = '';
  let count = 0;
  GARGALO_ORDER.forEach(status => {{
    const rows = groups[status] || [];
    if (rows.length === 0) return;
    const cor = GARGALO_COLORS[status] || '#6b7280';
    const totalFalta = rows.reduce((s,r)=>s+(r.falta_estimada||0),0);
    html += `<tr class="grp-hdr"><td colspan="16"
        style="border-left:4px solid ${{cor}};">
      ${{badge(status, cor)}}
      <span style="margin-left:12px;color:var(--muted);font-weight:400">${{rows.length}} produto(s)</span>
      ${{totalFalta>0?`<span style="margin-left:16px;color:${{cor}};font-weight:400">Falta total: <strong>${{num(totalFalta)}}</strong> unidades</span>`:''}}
    </td></tr>`;
    rows.forEach(r => {{
      html += `<tr>
        <td title="${{r.descricao}}">${{r.descricao||'-'}}</td>
        <td>${{r.marca||'-'}}</td>
        <td style="text-align:right">${{num(r.demanda_total)}}</td>
        <td style="text-align:right;color:${{r.saldo_atual<0?'#ef4444':'inherit'}}">${{num(r.saldo_atual)}}</td>
        <td style="text-align:right">${{num(r.estoque_futuro)}}</td>
        <td style="text-align:right;color:${{r.saldo_apos_pedidos<0?'#ef4444':'#22c55e'}}">${{num(r.saldo_apos_pedidos)}}</td>
        <td style="text-align:right;font-weight:700;color:${{r.falta_estimada>0?'#ef4444':'#22c55e'}}">${{num(r.falta_estimada)}}</td>
        <td style="text-align:right">${{num(r.media_mensal)}}</td>
        <td style="text-align:right;color:${{r.cobertura_meses<1?'#ef4444':r.cobertura_meses<3?'#f59e0b':'#22c55e'}}">${{r.cobertura_meses==99?'∞':r.cobertura_meses}}</td>
        <td style="text-align:right">${{num(r.qtde_pintados)}}</td>
        <td style="text-align:right">${{num(r.qtde_frascos)}}</td>
        <td style="text-align:right">${{num(r.envase_pendente)}}</td>
        <td style="text-align:center">${{num(r.n_pedidos)}}</td>
        <td style="text-align:center">${{num(r.n_clientes)}}</td>
        <td>${{badge(status,cor)}}</td>
        <td style="font-size:12px">${{r.acao_sugerida||'-'}}</td>
      </tr>`;
      count++;
    }});
  }});
  document.getElementById('tbody-gargalos').innerHTML = html;
  const c = document.getElementById('count-gargalos');
  if(c) c.textContent = count + ' produto(s)';
}}

function initGargalos() {{
  renderGargalosGrouped(GARGALOS);
}}

// ─── PAGE 5: PLANO ───────────────────────────────────────────────────────────
const PLANO_SECTIONS = [
  {{ key:'separar_agora', label:'🟢 Separar Agora', color:'#22c55e', cols:['pedido','cliente','vendedor','descricao','qtde_pedido','etapa'] }},
  {{ key:'separar_parcial', label:'🟡 Separar Parcialmente', color:'#f59e0b', cols:['pedido','cliente','vendedor','descricao','qtde_pedido','qtde_possivel','qtde_faltante'] }},
  {{ key:'envasar', label:'🟠 Mandar para Envase', color:'#f97316', cols:['pedido','cliente','descricao','qtde_pedido','qtde_faltante','est_pintados'] }},
  {{ key:'pintar', label:'🟣 Mandar para Pintura', color:'#8b5cf6', cols:['pedido','cliente','descricao','qtde_pedido','qtde_faltante','est_frascos'] }},
  {{ key:'comprar', label:'🔴 Comprar / Solicitar', color:'#ef4444', cols:['pedido','cliente','descricao','qtde_pedido','qtde_faltante'] }},
  {{ key:'validar_comercial', label:'🔵 Validar com Comercial', color:'#3b82f6', cols:['pedido','cliente','vendedor','descricao','qtde_pedido','qtde_faltante','demanda_total_sku'] }},
];

const COL_LABELS = {{
  pedido:'Pedido', cliente:'Cliente', vendedor:'Vendedor', descricao:'Produto',
  qtde_pedido:'Qtde Pedida', etapa:'Etapa', qtde_possivel:'Qtde Possível',
  qtde_faltante:'Qtde Faltante', est_pintados:'Pintados Disp.', est_frascos:'Frascos Disp.',
  demanda_total_sku:'Demanda Total SKU'
}};

function initPlano() {{
  const container = document.getElementById('plano-container');
  PLANO_SECTIONS.forEach(sec => {{
    const items = PLANO[sec.key] || [];
    if (items.length === 0) return;
    let html = `<div class="plano-section">
      <h3 style="background:${{sec.color}}22;color:${{sec.color}};border:1px solid ${{sec.color}}44">
        ${{sec.label}} &nbsp;<span style="font-weight:400;font-size:12px">(${{items.length}} item(s))</span>
      </h3>
      <div style="overflow:auto;background:var(--surface);border:1px solid var(--border);border-top:none;border-radius:0 0 6px 6px">
      <table class="plano-table">
        <thead><tr>${{sec.cols.map(c=>`<th>${{COL_LABELS[c]||c}}</th>`).join('')}}</tr></thead>
        <tbody>`;
    items.forEach(r => {{
      html += `<tr>${{sec.cols.map(c => {{
        const v = r[c];
        if (c==='descricao') return `<td title="${{v}}">${{v?.length>40?v.slice(0,40)+'…':v||'-'}}</td>`;
        if (typeof v === 'number') return `<td style="text-align:right">${{num(v)}}</td>`;
        return `<td>${{v||'-'}}</td>`;
      }}).join('')}}</tr>`;
    }});
    html += `</tbody></table></div></div>`;
    container.innerHTML += html;
  }});
}}

// ─── PAGE 6: DIAGNÓSTICO ─────────────────────────────────────────────────────
function initDiagnostico() {{
  const container = document.getElementById('diag-container');
  if (!DIAGNOSTICO || DIAGNOSTICO.length === 0) {{
    container.innerHTML = '<p style="color:var(--green)">Nenhum problema crítico identificado.</p>';
    return;
  }}
  const grouped = {{}};
  DIAGNOSTICO.forEach(d => {{
    if (!grouped[d.tipo]) grouped[d.tipo] = [];
    grouped[d.tipo].push(d);
  }});
  Object.entries(grouped).forEach(([tipo, items]) => {{
    container.innerHTML += `<h3 style="font-size:13px;color:var(--muted);margin:14px 0 6px">
      ${{tipo}} <span style="color:var(--red)">(${{items.length}})</span></h3>`;
    items.slice(0,50).forEach(d => {{
      container.innerHTML += `<div class="issue-card">
        <div class="tipo">${{d.tipo}}</div>
        <div style="font-weight:500">${{d.descricao}}</div>
        <div style="color:var(--muted);font-size:12px;margin-top:2px">${{d.acao}}</div>
      </div>`;
    }});
    if (items.length > 50) container.innerHTML += `<p style="color:var(--muted);font-size:12px;margin-bottom:8px">... e mais ${{items.length-50}} ocorrências</p>`;
  }});
}}

// Init first page
tablesInit['executivo'] = true;

// Remove jQuery/DataTables loading - not needed anymore for grouped pages
// (Chart.js still loaded, jQuery kept for potential compatibility)
</script>
</body>
</html>"""

    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"HTML gerado: {OUTPUT_HTML}")


# ─── MAIN ────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Dashboard de Planejamento de Pedidos & Estoque")
    print(f"Fonte de dados: {FONTE.upper()}")
    print("=" * 60)

    # ── Carregar dados conforme a fonte configurada ──────────────────────────
    if FONTE == "sheets":
        df_ped_raw, df_est_raw = carregar_do_sheets()
        pedidos = carregar_pedidos(df_raw=df_ped_raw)
        estoque = carregar_estoque(df_raw=df_est_raw)
    else:
        print("Usando arquivos locais...")
        pedidos = carregar_pedidos()
        estoque = carregar_estoque()

    # ── Pipeline de análise ──────────────────────────────────────────────────
    df_merged, sem_match = join_bases(pedidos, estoque)
    df_merged = classificar(df_merged)
    df_alocacao = calcular_alocacao(df_merged)
    df_gargalos = calcular_gargalos(df_merged, estoque)
    plano_acao = calcular_plano_acao(df_merged)
    diagnostico = calcular_diagnostico(pedidos, sem_match, df_merged)
    kpis = calcular_kpis(df_merged)

    gerar_html(kpis, df_merged, df_alocacao, df_gargalos, plano_acao, diagnostico)

    print("\nDashboard gerado com sucesso!")
    print(f"  Arquivo: {OUTPUT_HTML}")
    print(f"  Pedidos processados: {len(df_merged)}")
    print(f"  SKUs analisados: {df_merged['desc_norm'].nunique()}")
    print(f"  Produtos sem match: {len(sem_match)}")
    print(f"  SKUs em disputa: {df_merged['disputa'].sum()}")
    print(f"\n  STATUS SUMMARY:")
    for s, c in df_merged["status"].value_counts().items():
        print(f"    {s}: {c}")


if __name__ == "__main__":
    main()
