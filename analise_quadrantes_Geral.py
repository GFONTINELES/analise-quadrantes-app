# analise_quadrantes_Geral.py
import streamlit as st
import pandas as pd
import os
import glob
from streamlit_option_menu import option_menu
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import requests
import io
from typing import List

st.set_page_config(layout="wide", page_title=" ", )

menu_select = option_menu(None, ["Geral", "Gerencial"], icons=["house", "bar-chart-line"],
                          menu_icon="cast", default_index=0, orientation="horizontal")
df_filtrado = []

# ===============================
# CONFIGURAÇÃO DO CAMINHO BASE
# ===============================
# Repositório raw do GitHub (ajuste se for outro)
BASE_URL = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main"

# Se os arquivos estiverem em /exports/YYYY-MM-DD/ use essa constante como prefixo:
EXPORTS_PREFIX = f"{BASE_URL}/exports"

# MAPA DE ANALISTAS E LOJAS (mantive seu mapeamento)
ANALISTAS = {
    "LUCAS": ["LUCAS_consolidado"],
    "BRUNO": ["BRUNO_consolidado"],
    "GABRIEL": ["GABRIEL_consolidado"],
}

quadrante_nomes = {
    0: "Produtos Ok",
    1: "Estoque Excedente",
    2: "Falsa Ruptura - (Negativos)",
    3: "Produto Virtual",
    4: "Em Falta - (Ruptura)",
    5: "Divergências com inventário",
    6: "Curva A e B",
    7: "Outro 1",
    8: "Outro 2 "
}

# ===============================
# HELPERS para ler parquet via HTTP
# ===============================
def read_parquet_from_url(url: str) -> pd.DataFrame:
    """
    Faz GET no URL e tenta ler parquet via bytes (usa pyarrow/fastparquet via pandas).
    Retorna DataFrame ou levanta exceção.
    """
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return pd.read_parquet(io.BytesIO(resp.content))


# ===============================
# FUNÇÕES AUXILIARES (caching)
# ===============================
@st.cache_data
def listar_datas_online(days_back: int = 30) -> List[str]:
    """
    Tenta detectar pastas de data no repositório GitHub, assumindo estrutura:
    BASE_URL/exports/YYYY-MM-DD/<NOME_arquivo>.parquet
    Verifica existência de LUCAS_consolidado.parquet como verificação.
    Retorna lista de datas encontradas (strings YYYY-MM-DD) ordenadas desc.
    Se não encontrar nada, retorna ["main"] (uso dos arquivos no root).
    """
    found = []
    today = datetime.utcnow().date()
    for i in range(days_back + 1):
        d = today - timedelta(days=i)
        ds = d.strftime("%Y-%m-%d")
        url_check = f"{EXPORTS_PREFIX}/{ds}/LUCAS_consolidado.parquet"
        try:
            r = requests.head(url_check, timeout=8)
            if r.status_code == 200:
                found.append(ds)
        except requests.RequestException:
            # ignorar problemas de rede breves
            pass
    if not found:
        # as fallback, checar se os arquivos estão no root (main)
        # retornamos "main" para indicar uso do root (BASE_URL)
        return ["main"]
    # ordenar decrescente (mais recente primeiro)
    found.sort(reverse=True)
    return found


@st.cache_data
def listar_arquivos_online(data_label: str) -> List[str]:
    """
    Retorna lista de URLs de .parquet para uma dada 'data_label'.
    - se data_label == "main": retorna os 3 arquivos no root
    - se for YYYY-MM-DD: retorna arquivos dentro de /exports/YYYY-MM-DD/
    """
    urls = []
    if data_label == "main":
        # arquivos esperados no root
        urls = [
            f"{BASE_URL}/LUCAS_consolidado.parquet",
            f"{BASE_URL}/GABRIEL_consolidado.parquet",
            f"{BASE_URL}/BRUNO_consolidado.parquet",
        ]
    else:
        # verifica folder por data
        urls = [
            f"{EXPORTS_PREFIX}/{data_label}/LUCAS_consolidado.parquet",
            f"{EXPORTS_PREFIX}/{data_label}/GABRIEL_consolidado.parquet",
            f"{EXPORTS_PREFIX}/{data_label}/BRUNO_consolidado.parquet",
        ]
    # checar existência (HEAD) e devolver somente os que existem
    valid = []
    for u in urls:
        try:
            r = requests.head(u, timeout=8)
            if r.status_code == 200:
                valid.append(u)
        except requests.RequestException:
            continue
    return valid


@st.cache_data
def carregar_df_urls(urls: List[str]) -> List[pd.DataFrame]:
    """
    Recebe lista de URLs e retorna lista de DataFrames lidos.
    Adiciona coluna 'origem_arquivo' e tenta padronizar colunas essenciais.
    """
    dfs = []
    for u in urls:
        try:
            df = read_parquet_from_url(u)
            # tenta identificar o analista pelo nome do arquivo
            fname = os.path.basename(u).upper()
            if "LUCAS" in fname:
                df["analista_origem"] = "LUCAS"
            elif "BRUNO" in fname:
                df["analista_origem"] = "BRUNO"
            elif "GABRIEL" in fname:
                df["analista_origem"] = "GABRIEL"
            else:
                df["analista_origem"] = fname
            # garantir colunas que você usa depois
            if "Motivos" not in df.columns:
                df["Motivos"] = ""
            if "Areas" not in df.columns:
                df["Areas"] = ""
            if "Causa" not in df.columns:
                df["Causa"] = ""
            if "Vlr Recuperado" not in df.columns:
                df["Vlr Recuperado"] = 0
            dfs.append(df)
        except Exception as e:
            st.warning(f"Falha ao carregar {u}: {e}")
    return dfs


def carregar_planilha(df):
    """Mantive sua função de carregamento/normalização (não cacheada)"""
    if "Areas" not in df.columns:
        df["Areas"] = ""
    if "Causa" not in df.columns:
        df["Causa"] = ""
    if "Motivos" not in df.columns:
        df["Motivos"] = ""
    if "Vlr Recuperado" not in df.columns:
        df["Vlr Recuperado"] = ""
    return df


def filtrar_planilhas_por_analista(arquivos: List[str], analista: str) -> List[str]:
    """Filtra os arquivos (URLs) conforme string do analista no nome do arquivo."""
    lojas = ANALISTAS.get(analista, [])
    arquivos_filtrados = []
    for caminho in arquivos:
        nome_arquivo = os.path.basename(caminho)
        for loja in lojas:
            if loja.lower() in nome_arquivo.lower():
                arquivos_filtrados.append(caminho)
                break
    return arquivos_filtrados


# ===============================
# FUNÇÕES DE EXIBIÇÃO (mantidas)
# ===============================
def create_metric_chart_x(df, row, column, color, chart_type="Bar", height=190):
    chart_data = df.copy()
    if chart_type == 'Bar':
        try:
            st.bar_chart(chart_data, x=row, y=column, color=color, height=height)
        except Exception:
            # fallback simples
            st.write("Gráfico de barra indisponível para esses dados.")
    elif chart_type == 'Area':
        try:
            st.area_chart(chart_data, y=column, color=color, height=height)
        except Exception:
            st.write("Gráfico de área indisponível para esses dados.")


def display_metric_x(title, value, previous_value, df, row, column, color, chart_type="Bar"):
    try:
        delta = ((value - previous_value) / previous_value) * 100 if previous_value != 0 else 0
    except Exception:
        delta = 0

    with st.container():
        st.metric(
            label=title,
            value=f"{value:,.2f}%",
            delta=f"{delta:+.1f}%",
            delta_color="normal"
        )
        create_metric_chart_x(df, row, column, color, chart_type=chart_type, height=150)


def n_formatado(n):
    e_negativo = n < 0
    n = abs(n)
    if n >= 1_000_000_000:
        formatado = f"{n/1_000_000_000:.2f} bilhão"
    elif n >= 1_000_000:
        formatado = f"{n / 1_000_000:.2f} milhão"
    elif n >= 1_000:
        formatado = f"{n / 1_000:.2f} mil"
    else:
        formatado = f"{n:.2f}"
    formatado = formatado.replace(".", ",")
    if e_negativo:
        formatado = f"-{formatado}"
    return formatado


# ===============================
# INTERFACE STREAMLIT (fluxo principal)
# ===============================
st.title("📊 Relatorios Gerencial de Quadrantes")

# Tenta listar datas (pastas /exports/YYYY-MM-DD/) nos últimos 60 dias
with st.spinner("Detectando datas disponíveis..."):
    datas = listar_datas_online(days_back=60)

data_escolhida = st.selectbox("Selecione a data:", datas)
# se retornou "main", interpretamos como leitura dos arquivos no root
is_main_mode = (data_escolhida == "main")

# Convert string to datetime when possible (para cálculo de dia anterior)
try:
    data_datetime = datetime.strptime(data_escolhida, "%Y-%m-%d").date() if not is_main_mode else datetime.utcnow().date()
except Exception:
    data_datetime = datetime.utcnow().date()

dia_anterior = data_datetime - timedelta(days=1)
dia_anterior_str = dia_anterior.strftime("%Y-%m-%d")

# Monta lista de URLs para a data selecionada (ou root)
arquivos_para_data = listar_arquivos_online(data_escolhida)

# Se não encontrou arquivos para a data, avisa e tenta fallback para root
if not arquivos_para_data:
    st.warning("Não foram encontrados arquivos para a data selecionada. Tentando usar os arquivos no root do repositório...")
    arquivos_para_data = listar_arquivos_online("main")
    is_main_mode = True

# Carrega os dataframes a partir das URLs
dfs = carregar_df_urls(arquivos_para_data)
if not dfs:
    st.error("Nenhum arquivo .parquet pôde ser carregado a partir das URLs detectadas. Verifique o repo e os nomes dos arquivos.")
    st.stop()

# Concatenar em df_final (manter lógica original)
df_final = pd.concat(dfs, axis=0, ignore_index=True)

# Limpa valores nulos e None na coluna Motivos
if "Motivos" in df_final.columns:
    df_final["Motivos"] = df_final["Motivos"].fillna("").replace("None", "")

# Garante tipo numérico em 'Vlr Recuperado'
if "Vlr Recuperado" in df_final.columns:
    df_final["Vlr Recuperado"] = pd.to_numeric(df_final["Vlr Recuperado"], errors="coerce").astype("Int64")
else:
    df_final["Vlr Recuperado"] = 0

# Aplica nome dos quadrantes (se coluna existir)
if "quadrante" in df_final.columns:
    df_final["quadrante"] = df_final["quadrante"].replace(quadrante_nomes)

# Agora carregamos os arquivos do dia anterior (para comparações históricas)
# tentamos a mesma lógica: buscar na pasta de exports ou no root
if not is_main_mode:
    arquivos_dia_anterior = listar_arquivos_online(dia_anterior_str)
    if not arquivos_dia_anterior:
        # fallback para root
        arquivos_dia_anterior = listar_arquivos_online("main")
else:
    arquivos_dia_anterior = listar_arquivos_online("main")

dfs_dia_anterior = carregar_df_urls(arquivos_dia_anterior)
if dfs_dia_anterior:
    df_final_2 = pd.concat(dfs_dia_anterior, axis=0, ignore_index=True)
    if "quadrante" in df_final_2.columns:
        df_final_2["quadrante"] = df_final_2["quadrante"].replace(quadrante_nomes)
    if "Motivos" in df_final_2.columns:
        df_final_2["Motivos"] = df_final_2["Motivos"].fillna("").replace("None", "")
    if "Vlr Recuperado" in df_final_2.columns:
        df_final_2["Vlr Recuperado"] = pd.to_numeric(df_final_2["Vlr Recuperado"], errors="coerce").astype("Int64")
else:
    # cria df_final_2 vazio com mesmas colunas (se possível)
    df_final_2 = pd.DataFrame(columns=df_final.columns)

# ===============================
# Lógica de visões e gráficos (preservando seu fluxo)
# ===============================
visoes = ['quadrante', 'DESCRDIVISAO_x']
st.title("Dashboard")

for tipe in visoes:
    # 🔹 Base groupings
    if tipe not in df_final.columns:
        st.warning(f"A coluna {tipe} não existe nos dados. Pulei essa visão.")
        continue

    df_quandant = (
        df_final.groupby([f"{tipe}"])
        .agg({"Vlr Recuperado": "sum"})
        .reset_index()
    )

    df_total_itens = (
        df_final.groupby([f"{tipe}"])
        .size()
        .reset_index(name="TOTAL_ITENS")
    )

    df_motivos = (
        df_final.groupby([f"{tipe}", "Motivos"])
        .agg({"Vlr Recuperado": "sum"})
        .reset_index()
    )

    df_so_motivos = (
        df_motivos[df_motivos["Motivos"].astype(str).str.strip() != ""]
        .dropna(subset=["Motivos"])
    )

    df_itens_analizados = (
        df_final[df_final["Motivos"].astype(str).str.strip() != ""]
        .groupby([f"{tipe}"])
        .size()
        .reset_index(name="ITENS_ANALISADOS")
    )

    df_so_motivos = (
        df_so_motivos.groupby([f"{tipe}"])
        .agg({"Vlr Recuperado": "sum"})
        .reset_index()
    )

    df_Quadra_Final = (
        df_quandant.merge(df_so_motivos, how="left", on=[f"{tipe}"])
        .rename(
            columns={
                "CUSTOTOTAL_x": "Vlr Recuperado",
                "CUSTOTOTAL_y": "CUSTO_TOTAL_ANALISADO",
            }
        )
    )

    # -----------------------------------------------------------------------------------------------------------------------------
    if not df_final_2.empty and tipe in df_final_2.columns:
        df_quandant2 = (
            df_final_2.groupby([f"{tipe}"])
            .agg({"Vlr Recuperado": "sum"})
            .reset_index()
        )

        df_total_itens2 = (
            df_final_2.groupby([f"{tipe}"])
            .size()
            .reset_index(name="TOTAL_ITENS")
        )

        df_motivos2 = (
            df_final_2.groupby([f"{tipe}", "Motivos"])
            .agg({"Vlr Recuperado": "sum"})
            .reset_index()
        )

        df_so_motivos2 = (
            df_motivos2[df_motivos2["Motivos"].astype(str).str.strip() != ""]
            .dropna(subset=["Motivos"])
        )

        df_itens_analizados2 = (
            df_final_2[df_final_2["Motivos"].astype(str).str.strip() != ""]
            .groupby([f"{tipe}"])
            .size()
            .reset_index(name="ITENS_ANALISADOS")
        )

        df_so_motivos2["Vlr Recuperado"] = pd.to_numeric(df_so_motivos2["Vlr Recuperado"], errors="coerce").fillna(0)

        df_so_motivos2 = (df_so_motivos2.groupby([f"{tipe}"]).agg({"Vlr Recuperado": "sum"}).reset_index())

        df_Quadra_Final2 = (
            df_quandant2.merge(df_so_motivos2, how="left", on=[f"{tipe}"])
            .rename(
                columns={
                    "CUSTOTOTAL_x": "Vlr Recuperado",
                    "CUSTOTOTAL_y": "CUSTO_TOTAL_ANALISADO",
                }
            )
        )

        if "resolvido" in df_final_2.columns:
            df_resolvido2 = (
                df_final_2.groupby([f"{tipe}"])["resolvido"]
                .apply(lambda x: (x == True).sum())
                .reset_index(name="RESOLVIDO_CONTAGEM")
            )
            df_Quadra_Final2 = df_Quadra_Final2.merge(df_resolvido2, how="left", on=[f"{tipe}"])

        df_Quadra_Final2 = (
            df_Quadra_Final2
            .merge(df_total_itens2, how="left", on=[f"{tipe}"])
            .merge(df_itens_analizados2, how="left", on=[f"{tipe}"])
        )
    else:
        # criar df_Quadra_Final2 com zeros para evitar erros posteriores
        df_Quadra_Final2 = pd.DataFrame(columns=["TOTAL_ITENS", "ITENS_ANALISADOS", "Vlr Recuperado", f"{tipe}"])

    # ===============================
    # Add "Resolvido" count
    # ===============================
    if "resolvido" in df_final.columns:
        df_resolvido = (
            df_final.groupby([f"{tipe}"])["resolvido"]
            .apply(lambda x: (x == True).sum())
            .reset_index(name="RESOLVIDO_CONTAGEM")
        )
        df_Quadra_Final = df_Quadra_Final.merge(df_resolvido, how="left", on=[f"{tipe}"])
    else:
        df_Quadra_Final["RESOLVIDO_CONTAGEM"] = 0

    # ===============================
    # Merge item counts
    # ===============================
    df_Quadra_Final = (
        df_Quadra_Final
        .merge(df_total_itens, how="left", on=[f"{tipe}"])
        .merge(df_itens_analizados, how="left", on=[f"{tipe}"])
    )

    # convert values and compute sums
    def convert_to_percentage(value):
        if pd.isna(value):
            return ''
        else:
            return f"{value * 100:.2f}%"

    sum_total_qua = df_Quadra_Final['TOTAL_ITENS'].sum()

    if tipe == "quadrante":
        sum_somente_fora = df_Quadra_Final[df_Quadra_Final[f'{tipe}'] != 'Produtos Ok']
        sum_somente_dentro = df_Quadra_Final[df_Quadra_Final[f'{tipe}'] == 'Produtos Ok']

        sum_somente_fora2 = df_Quadra_Final2[df_Quadra_Final2[f'{tipe}'] != 'Produtos Ok'] if not df_Quadra_Final2.empty else pd.DataFrame()
        sum_somente_dentro2 = df_Quadra_Final2[df_Quadra_Final2[f'{tipe}'] == 'Produtos Ok'] if not df_Quadra_Final2.empty else pd.DataFrame()

        sum_somente_fora2 = sum_somente_fora2["TOTAL_ITENS"].sum() if not sum_somente_fora2.empty else 0
        sum_somente_dentro2 = sum_somente_dentro2["TOTAL_ITENS"].sum() if not sum_somente_dentro2.empty else 0

        sum_somente_fora = sum_somente_fora["TOTAL_ITENS"].sum()
        sum_somente_dentro = sum_somente_dentro["TOTAL_ITENS"].sum()

        # evitar divisão por zero
        div_final = (sum_somente_dentro / sum_somente_fora) if sum_somente_fora != 0 else 0
        div_final2 = (sum_somente_dentro2 / sum_somente_fora2) if sum_somente_fora2 != 0 else 0

        valor_atual = div_final * 100
        valor_anterior = div_final2 * 100

        display_metric_x(
            title="Vendas Diárias",
            value=valor_atual,
            previous_value=valor_anterior,
            df=df_Quadra_Final,
            row="Dia",
            column="Vendas",
            color=["#1f77b4"],
            chart_type=""
        )

    # ===============================
    # Display (texto + gráficos)
    # ===============================
    total_itens_sum = int(df_Quadra_Final["TOTAL_ITENS"].sum()) if "TOTAL_ITENS" in df_Quadra_Final.columns else 0
    total_analisados_sum = int(df_Quadra_Final["ITENS_ANALISADOS"].sum()) if "ITENS_ANALISADOS" in df_Quadra_Final.columns else 0
    acuracidade = round((total_analisados_sum / total_itens_sum) * 100, 2) if total_itens_sum != 0 else 0

    st.subheader(f'Visão: {tipe} com total:{total_itens_sum} / Analisados:{total_analisados_sum} Com Acuracidade de {acuracidade}%')

    # selecionar colunas para exibição, tentamos manter a estrutura original
    expected_cols = [f"{tipe}", "RESOLVIDO_CONTAGEM", "TOTAL_ITENS", "ITENS_ANALISADOS"]
    # pegar coluna de valor recuperado renomeada se existir
    val_col_candidates = [c for c in df_Quadra_Final.columns if "Vlr Recuperado" in c or "Vlr Recuperado_x" in c]
    if val_col_candidates:
        expected_cols.append(val_col_candidates[0])
    else:
        # fallback: tentar adicionar Vlr Recuperado
        if "Vlr Recuperado" in df_Quadra_Final.columns:
            expected_cols.append("Vlr Recuperado")

    # filtrar colunas que existem
    expected_cols = [c for c in expected_cols if c in df_Quadra_Final.columns]
    if not expected_cols:
        st.write("Sem colunas para exibir nesta visão.")
        continue

    df_display = df_Quadra_Final[expected_cols].copy()
    selecao = st.dataframe(df_display, on_select="rerun")

    tipo_analis_fig1 = st.selectbox(f"Escolha a visão {tipe}", ["ITENS_ANALISADOS", "RESOLVIDO_CONTAGEM"])
    fig = go.Figure()

    # barras: total itens, analisados, resolvido
    fig.add_trace(go.Bar(
        x=df_display[f"{tipe}"],
        y=df_display["TOTAL_ITENS"] if "TOTAL_ITENS" in df_display.columns else [0] * len(df_display),
        name="Total Itens",
        marker_color="royalblue", text=df_display.get("TOTAL_ITENS")
    ))

    if "ITENS_ANALISADOS" in df_display.columns:
        fig.add_trace(go.Bar(
            x=df_display[f"{tipe}"],
            y=df_display["ITENS_ANALISADOS"],
            name="ITENS_ANALISADOS",
            marker_color="green", text=df_display["ITENS_ANALISADOS"]
        ))

    if "RESOLVIDO_CONTAGEM" in df_display.columns:
        fig.add_trace(go.Bar(
            x=df_display[f"{tipe}"],
            y=df_display["RESOLVIDO_CONTAGEM"],
            name="RESOLVIDO_CONTAGEM",
            marker_color="yellow", text=df_display["RESOLVIDO_CONTAGEM"]
        ))

    fig.update_layout(
        barmode="group",
        title="Itens Totais vs Analisados por " + tipe,
        xaxis_title=tipe,
        yaxis_title="Quantidade de Itens",
        template="plotly_white"
    )

    # Pie de valor recuperado (se existir)
    pie_val_col = val_col_candidates[0] if val_col_candidates else None
    if pie_val_col:
        fig_2 = px.pie(
            df_display,
            names=f"{tipe}",
            values=pie_val_col,
            hole=0.4,
            title=f"Valor Recuperado por {tipe}"
        )
        fig_2.update_traces(textinfo="percent+label")
    else:
        fig_2 = None

    col_1, col_2 = st.columns([0.50, 0.50])
    with col_2:
        if fig_2:
            st.plotly_chart(fig_2, use_container_width=True)
    with col_1:
        st.plotly_chart(fig, use_container_width=True)

    # lógica de seleção e detalhamento por loja (preservada)
    if selecao:
        try:
            selected_rows = selecao["selection"]["rows"]
            if selected_rows:
                selected_index = selected_rows[0]
                nome_quadrante = df_display.iloc[selected_index][f"{tipe}"]
                df_geral_filtrado = df_final.loc[df_final[f"{tipe}"] == nome_quadrante]
                df_loja = df_geral_filtrado.groupby(["IDEMPRESA"]).agg({"resolvido": "count", }).reset_index()

                sele_lojas = st.selectbox(F"escolha a loja {tipe}", df_geral_filtrado["IDEMPRESA"].unique())
                # --- 1️⃣ Group and count ---
                df_grouped = (
                    df_geral_filtrado.groupby("IDEMPRESA")
                    .agg(
                        TOTAL_ITENS=("IDEMPRESA", "count"),
                        RESOLVIDOS=("resolvido", lambda x: (x == True).sum())
                    )
                    .reset_index()
                )
                df_grouped = df_grouped.loc[df_grouped["IDEMPRESA"] == sele_lojas]
                # --- 2️⃣ Prepare for chart ---
                df_melted = df_grouped.melt(
                    id_vars="IDEMPRESA",
                    value_vars=["TOTAL_ITENS", "RESOLVIDOS"],
                    var_name="Tipo",
                    value_name="Quantidade"
                )
                fig_final_loja = px.bar(
                    df_melted,
                    x="IDEMPRESA",
                    y="Quantidade",
                    color="Tipo",
                    barmode="group",
                    text_auto=True,
                    title="Comparativo de Itens Resolvidos x Totais por Loja",
                )
                fig_final_loja.update_layout(xaxis_title="IDEMPRESA (Loja)", yaxis_title="Quantidade de Itens")
                st.plotly_chart(fig_final_loja, use_container_width=True)
        except Exception as e:
            st.warning(f"Erro ao tratar seleção: {e}")


# ===============================
# Caso menu_select seja "Gerencial" (preservei sua segunda aba)
# ===============================
if menu_select != "Geral":
    # Aqui podemos manter o mesmo fluxo usado no else do seu script original: filtragem por analista
    if data_escolhida:
        analista = st.selectbox("Analistas:", list(ANALISTAS.keys()))
        # arquivos para a data selecionada
        arquivos = listar_arquivos_online(data_escolhida)
        arquivos_filtrados = filtrar_planilhas_por_analista(arquivos, analista)

        if not arquivos_filtrados:
            st.warning("Nenhuma planilha encontrada para este analista na data selecionada.")
        else:
            st.subheader(f"Planilhas encontradas para {analista}:")
            for a in arquivos_filtrados:
                st.write(a)

    # Carrega todos os três (fallback para root se necessário)
    arquivos_root = listar_arquivos_online("main")
    dfs_root = carregar_df_urls(arquivos_root)
    if dfs_root:
        df_root = pd.concat(dfs_root, axis=0, ignore_index=True)
    else:
        df_root = pd.DataFrame()

    # exemplo de exibição semelhante à sua lógica original
    if not df_root.empty:
        df_root["Vlr Recuperado"] = pd.to_numeric(df_root["Vlr Recuperado"], errors="coerce")
        df_root["quadrante"] = df_root["quadrante"].replace(quadrante_nomes) if "quadrante" in df_root.columns else df_root.get("quadrante", "")
        st.subheader("Dados Gerencial (root)")
        st.dataframe(df_root.head(20))
    else:
        st.warning("Dados gerenciais não disponíveis.")

# Fim do app
