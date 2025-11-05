import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta

# ======================================================
# CONFIGURAÇÕES GERAIS
# ======================================================
st.set_page_config(page_title="Análise de Quadrantes", layout="wide")
st.title("📊 Análise de Quadrantes - Gestão de Estoque")

# Caminho base (repositório GitHub com os arquivos .parquet)
BASE_URL = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main"

# Dicionário com os arquivos dos analistas
ANALISTAS = {
    "LUCAS": f"{BASE_URL}/LUCAS_consolidado.parquet",
    "GABRIEL": f"{BASE_URL}/GABRIEL_consolidado.parquet",
    "BRUNO": f"{BASE_URL}/BRUNO_consolidado.parquet",
}

# ======================================================
# FUNÇÕES AUXILIARES
# ======================================================

@st.cache_data
def carregar_dados():
    """Carrega todos os DataFrames dos analistas a partir das URLs do GitHub"""
    dataframes = {}
    for nome, url in ANALISTAS.items():
        try:
            df = pd.read_parquet(url)
            df["analista"] = nome
            dataframes[nome] = df
        except Exception as e:
            st.warning(f"⚠️ Erro ao carregar {nome}: {e}")
    return dataframes

@st.cache_data
def unificar_dados(dfs_dict):
    """Concatena todos os DataFrames em um único"""
    if not dfs_dict:
        return pd.DataFrame()
    return pd.concat(dfs_dict.values(), ignore_index=True)

# ======================================================
# CARREGAMENTO DOS DADOS
# ======================================================

st.info("🔄 Carregando dados dos analistas diretamente do GitHub...")
dataframes = carregar_dados()
df_total = unificar_dados(dataframes)

if df_total.empty:
    st.error("❌ Nenhum dado foi carregado. Verifique os links do GitHub.")
    st.stop()

st.success("✅ Dados carregados com sucesso!")

# ======================================================
# MENU LATERAL
# ======================================================
with st.sidebar:
    menu_select = option_menu(
        "Menu Principal",
        ["Geral", "Por Analista", "Por Loja"],
        icons=["bar-chart", "person-circle", "shop"],
        menu_icon="menu-app",
        default_index=0,
    )

# ======================================================
# PÁGINA: VISÃO GERAL
# ======================================================
if menu_select == "Geral":
    st.header("📈 Visão Geral do Estoque")
    
    qtd_total = len(df_total)
    qtd_com_giro = len(df_total[df_total["média_mês"] > 0])
    qtd_sem_giro = qtd_total - qtd_com_giro

    col1, col2, col3 = st.columns(3)
    col1.metric("Total de Itens", f"{qtd_total:,}")
    col2.metric("Itens com Giro", f"{qtd_com_giro:,}")
    col3.metric("Itens sem Giro", f"{qtd_sem_giro:,}")

    fig = px.pie(
        names=["Com Giro", "Sem Giro"],
        values=[qtd_com_giro, qtd_sem_giro],
        title="Distribuição de Giro de Itens",
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(df_total.head(10))

# ======================================================
# PÁGINA: POR ANALISTA
# ======================================================
elif menu_select == "Por Analista":
    st.header("👤 Desempenho por Analista")

    analista_sel = st.selectbox("Selecione o analista:", list(ANALISTAS.keys()))
    df_analista = dataframes.get(analista_sel)

    if df_analista is not None:
        qtd = len(df_analista)
        giro_medio = df_analista["média_mês"].mean()
        cobertura_media = df_analista["cobertura_meses"].mean()

        col1, col2, col3 = st.columns(3)
        col1.metric("Itens Analisados", f"{qtd:,}")
        col2.metric("Giro Médio (mês)", f"{giro_medio:.2f}")
        col3.metric("Cobertura Média (meses)", f"{cobertura_media:.2f}")

        fig = px.histogram(
            df_analista,
            x="média_mês",
            nbins=30,
            title=f"Distribuição de Giro - {analista_sel}",
            color_discrete_sequence=["#00CC96"]
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df_analista.head(10))
    else:
        st.warning("⚠️ Nenhum dado disponível para este analista.")

# ======================================================
# PÁGINA: POR LOJA
# ======================================================
elif menu_select == "Por Loja":
    st.header("🏪 Análise por Loja")

    if "IDEMPRESA" not in df_total.columns:
        st.warning("⚠️ A coluna 'IDEMPRESA' não foi encontrada nos dados.")
    else:
        lojas = sorted(df_total["IDEMPRESA"].dropna().unique())
        loja_sel = st.selectbox("Selecione a loja:", lojas)

        df_loja = df_total[df_total["IDEMPRESA"] == loja_sel]

        if not df_loja.empty:
            fig = px.bar(
                df_loja,
                x="analista",
                y="média_mês",
                color="analista",
                title=f"Giro médio por analista - Loja {loja_sel}",
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(df_loja.head(10))
        else:
            st.warning("⚠️ Nenhum dado encontrado para esta loja.")
