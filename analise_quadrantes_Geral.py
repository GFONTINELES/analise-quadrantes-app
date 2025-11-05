import streamlit as st
import pandas as pd
import os
import glob
from streamlit_option_menu import option_menu
        
import plotly.graph_objects as go
import plotly.express as px
st.set_page_config(layout="wide",page_title=" ",
                   )
menu_select = option_menu(None,["Geral","Gerencial"],icons=["house","bar-chart-line"],menu_icon ="cast",default_index=0,orientation = "horizontal")
df_filtrado = []


# Custom CSS for green headers in DataFrames

# ===============================
# CONFIGURAÇÃO DO CAMINHO BASE
# ===============================
BASE_DIR = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main"

# MAPA DE ANALISTAS E LOJAS
ANALISTAS = {
    "LUCAS": ["LUCAS_consolidado"],
    "BRUNO": ["BRUNO_consolidado"],
    "GABRIEL": ["GABRIEL_consolidado"],   
    
}
#"LUCAS": ["Dom Severino", "Mocambinho", "Kennedy", "Lourival Parente", "Morada do Sol", "Matriz"]
#"LUCAS": ["Mocambinho", "Noé Mendes", "Porto Alegre", "PQ. Piauí", "São Joaquim", "CD ANEXO 2", "Distribuidora"]
#"LUCAS": ["Água Branca", "Água Mineral", "Atacarejo Cristo Rei", "Centro", "Demerval Lobão", "Dirceu", "Renascença"]}

def create_metric_chart_x(df, row, column, color, chart_type="Bar", height=190):
    chart_data = df.copy()
    if chart_type == 'Bar':
        st.bar_chart(chart_data, x=row, y=column, color=color, height=height)
    elif chart_type == 'Area':
        st.area_chart(chart_data, y=column, color=color, height=height)

def display_metric_x(title, value, previous_value, df, row, column, color, chart_type="Bar"):
    # Calcula delta (%)
    try:
        delta = ((value - previous_value) / previous_value) * 100 if previous_value != 0 else 0
    except:
        delta = 0

    # Exibe container da métrica
    with st.container(border=True):
        st.metric(
            label=title,
            value=f"{value:,.2f}%",
            delta=f"{delta:+.1f}%",
            delta_color="normal"  # Streamlit já pinta verde/vermelho automaticamente
        )
        create_metric_chart_x(df, row, column, color, chart_type=chart_type, height=150)

            
def n_formatado(n):
    e_negativo = n < 0
    n = abs(n)

    if n >= 1_000_000_000:
        formatado = f"{n/1_000_000_000:.2f} bilhão"
    elif n >= 1_000_000:  # Millions
        formatado = f"{n / 1_000_000:.2f} milhão"
    elif n >= 1_000:  # Thousands
        formatado = f"{n / 1_000:.2f} mil"
    else:  # Below 1000, just format normally
        formatado = f"{n:.2f}"
    
    formatado = formatado.replace(".",",")

    if e_negativo:
        formatado = f"-{formatado}"

    return formatado

        
quadrante_nomes = {0:"Produtos Ok",
    1: "Estoque Excedente",
    2: "Falsa Ruptura - (Negativos)",
    3: "Produto Virtual",
    4: "Em Falta - (Ruptura)",
    5: "Divergências com inventário",
    6: "Curva A e B", 
    7: "Outro 1",  # Optional: add names if you go beyond 6
    8: "Outro 2 "
}

# ===============================
# FUNÇÕES AUXILIARES
# ===============================
@st.cache_data
def listar_datas():
    """Lista as pastas de datas dentro de exports."""
    pastas = sorted(
        [p for p in os.listdir(BASE_DIR) if os.path.isdir(os.path.join(BASE_DIR, p))],
        reverse=True
    )
    return pastas

@st.cache_data
def listar_arquivos(data_path):
    """Lista todos os arquivos Excel dentro da pasta de uma data."""
    return glob.glob(os.path.join(data_path, "*.parquet"))
@st.cache_data
def carregar_planilha(df):
        if "Areas" not in df.columns:
            df["Areas"] = ""
        if "Causa" not in df.columns:
            df["Causa"] = ""
        if "Motivos" not in df.columns:
            df["Motivos"] = ""
        if "Vlr Recuperado" not in df.columns:
            df["Vlr Recuperado"] = ""
        return df
    

def filtrar_planilhas_por_analista(arquivos, analista):
    """Filtra os arquivos Excel conforme as lojas atribuídas ao analista."""
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
# INTERFACE STREAMLIT
# ===============================
st.title("📊 Relatorios Gerencial de Quadrantes")
from datetime import datetime, timedelta

datas = listar_datas()
data_escolhida = st.selectbox("Selecione a data:", datas)
# Convert string to datetime
data_datetime = datetime.strptime(data_escolhida, "%Y-%m-%d")

# Get the previous day
dia_anterior = data_datetime - timedelta(days=1)
if menu_select ==  "Geral":

     

    #C:\Users\Ferreira\OneDrive\Analises quadrantes\Lucas  
    url_LUCAS = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main/LUCAS_consolidado.parquet"
    df_LUCAS = pd.read_parquet(url_LUCAS)
    url_gabriel = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main/GABRIEL_consolidado.parquet"
    df_Gabriel = pd.read_parquet(url_gabriel)
    url_bruno = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main/BRUNO_consolidado.parquet"
    df_Bruno = pd.read_parquet(url_bruno)

    df_final = pd.concat([df_LUCAS,df_Gabriel,df_Bruno],axis=0) 
    
    # Limpa valores nulos e None na coluna Motivos
    df_final["Motivos"] = df_final["Motivos"].fillna("").replace("None", "")

    
    
    from typing import Generator, Sequence
    df_final["Vlr Recuperado"] = pd.to_numeric(df_final["Vlr Recuperado"], errors="coerce").astype("Int64")

    df_final["quadrante"] = df_final["quadrante"].replace(quadrante_nomes)

    data_escolhida_dt = pd.to_datetime(data_escolhida)
    dia_anterior_str = dia_anterior.strftime("%Y-%m-%d")

    df_Lucas_2 = pd.read_parquet(rf"C:\Users\FERREIRA\OneDrive\teste leitura\exports\{dia_anterior_str}\LUCAS_consolidado.parquet")
    df_Gabriel_2 = pd.read_parquet(rf"C:\Users\FERREIRA\OneDrive\teste leitura\exports\{dia_anterior_str}\GABRIEL_consolidado.parquet")
    df_Bruno_2 = pd.read_parquet(rf"C:\Users\FERREIRA\OneDrive\teste leitura\exports\{dia_anterior_str}\BRUNO_consolidado.parquet")

    df_final_2 = pd.concat([df_Lucas_2,df_Gabriel_2,df_Bruno_2],axis=0) 
    
    
    df_final_2["quadrante"] = df_final_2["quadrante"].replace(quadrante_nomes)
    df_final_2["Motivos"] = df_final_2["Motivos"].fillna("").replace("None", "")

    # Filtrar a data mais próxima **antes de data_escolhida**

    visoes = ['quadrante','DESCRDIVISAO_x']

    st.title("Dashboard")

    for tipe in visoes:
        # ===============================
        # 🔹 Base groupings
        # ===============================
        df_quandant = (
            df_final.groupby([f"{tipe}"])
            .agg({"Vlr Recuperado": "sum"})
            .reset_index()
        )
        
        # Count total items per group (all rows)
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

        # 🔹 Clean 'Motivos'
        df_so_motivos = (
            df_motivos[df_motivos["Motivos"].astype(str).str.strip() != ""]
            .dropna(subset=["Motivos"])
        )

        # Count how many items have Motivos filled (analysed)
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

        # 🔹 Merge to get totals
        df_Quadra_Final = (
            df_quandant.merge(df_so_motivos, how="left", on=[f"{tipe}"])
            .rename(
                columns={
                    "CUSTOTOTAL_x": "Vlr Recuperado",
                    "CUSTOTOTAL_y": "CUSTO_TOTAL_ANALISADO",
                }
            )
        )
        
        #-----------------------------------------------------------------------------------------------------------------------------
        df_quandant2 = (
            df_final_2.groupby([f"{tipe}"])
            .agg({"Vlr Recuperado": "sum"})
            .reset_index()
        )
        
        # Count total items per group (all rows)
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

        # 🔹 Clean 'Motivos'
        df_so_motivos2 = (
            df_motivos2[df_motivos2["Motivos"].astype(str).str.strip() != ""]
            .dropna(subset=["Motivos"])
        )

        # Count how many items have Motivos filled (analysed)
        df_itens_analizados2 = (
            df_final_2[df_final_2["Motivos"].astype(str).str.strip() != ""]
            .groupby([f"{tipe}"])
            .size()
            .reset_index(name="ITENS_ANALISADOS")
        )
        df_so_motivos2["Vlr Recuperado"] = pd.to_numeric(df_so_motivos2["Vlr Recuperado"], errors="coerce").fillna(0)

        df_so_motivos2 = (df_so_motivos2.groupby([f"{tipe}"]).agg({"Vlr Recuperado": "sum"}).reset_index()
        )

        # 🔹 Merge to get totals
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
            df_Quadra_Final2 = df_Quadra_Final2.merge(
                    df_resolvido2, how="left", on=[f"{tipe}"]
                )
            
            
        df_Quadra_Final2 = (
            df_Quadra_Final2
            .merge(df_total_itens2, how="left", on=[f"{tipe}"])
            .merge(df_itens_analizados2, how="left", on=[f"{tipe}"])
        )
        
        # ===============================
        # 🔹 Add "Resolvido" count
        # ===============================
        if "resolvido" in df_final.columns:
            df_resolvido = (
                df_final.groupby([f"{tipe}"])["resolvido"]
                .apply(lambda x: (x == True).sum())
                .reset_index(name="RESOLVIDO_CONTAGEM")
            )

            df_Quadra_Final = df_Quadra_Final.merge(
                df_resolvido, how="left", on=[f"{tipe}"]
            )
        else:
            df_Quadra_Final["RESOLVIDO_CONTAGEM"] = 0

        # ===============================
        # 🔹 Merge item counts
        # ===============================
        df_Quadra_Final = (
            df_Quadra_Final
            .merge(df_total_itens, how="left", on=[f"{tipe}"])
            .merge(df_itens_analizados, how="left", on=[f"{tipe}"])
        )
        
        
        def convert_to_percentage(value):
            if pd.isna(value):  # Check if the value is NaN
                return ''
            else:
                return f"{value * 100:.2f}%"  # Convert

        sum_total_qua = df_Quadra_Final['TOTAL_ITENS'].sum()
        
        if tipe == "quadrante":
            sum_somente_fora = df_Quadra_Final[df_Quadra_Final[f'{tipe}'] != 'Produtos Ok'] 
            sum_somente_dentro = df_Quadra_Final[df_Quadra_Final[f'{tipe}'] == 'Produtos Ok'] 
            
            sum_somente_fora2 = df_Quadra_Final2[df_Quadra_Final2[f'{tipe}'] != 'Produtos Ok'] 
            sum_somente_dentro2 = df_Quadra_Final2[df_Quadra_Final2[f'{tipe}'] == 'Produtos Ok'] 
            
            sum_somente_fora2 = sum_somente_fora2["TOTAL_ITENS"].sum()
            sum_somente_dentro2 = sum_somente_dentro2["TOTAL_ITENS"].sum()

            sum_somente_fora = sum_somente_fora["TOTAL_ITENS"].sum()
            sum_somente_dentro = sum_somente_dentro["TOTAL_ITENS"].sum()

            
            div_final = sum_somente_dentro/sum_somente_fora
            div_final2 = sum_somente_dentro2/sum_somente_fora2
    
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
        # 🔹 Display
        # ===============================
        
        st.subheader(f"Visão: {tipe} com total:{df_Quadra_Final["TOTAL_ITENS"].sum()} / Analisados:{int(df_Quadra_Final["ITENS_ANALISADOS"].sum())} Com Acuracidade de {round(df_Quadra_Final["ITENS_ANALISADOS"].sum()/int(df_Quadra_Final["TOTAL_ITENS"].sum()) * 100,2)}%")

        df_Quadra_Final = df_Quadra_Final[[F"{tipe}","RESOLVIDO_CONTAGEM","TOTAL_ITENS","ITENS_ANALISADOS","Vlr Recuperado_x"]]

        
        selecao = st.dataframe(df_Quadra_Final,on_select="rerun")

        tipo_analis_fig1 = st.selectbox(f"Escolha a visão {tipe}",["ITENS_ANALISADOS","RESOLVIDO_CONTAGEM"])
        fig = go.Figure()
    
        fig.add_trace(go.Bar(
            x=df_Quadra_Final[f"{tipe}"],
            y=df_Quadra_Final["TOTAL_ITENS"],
            name="Total Itens",
            marker_color="royalblue", text=df_Quadra_Final["TOTAL_ITENS"],
        textposition="outside"
        ))

        fig.add_trace(go.Bar(
            x=df_Quadra_Final[f"{tipe}"],
            y=df_Quadra_Final[f"ITENS_ANALISADOS"],
            name=f"ITENS_ANALISADOS",
            marker_color="green", text=df_Quadra_Final["ITENS_ANALISADOS"],
        textposition="outside"
        ))
        
        fig.add_trace(go.Bar(
            x=df_Quadra_Final[f"{tipe}"],
            y=df_Quadra_Final[f"RESOLVIDO_CONTAGEM"],
            name=f"RESOLVIDO_CONTAGEM",
            marker_color="yellow", text=df_Quadra_Final["RESOLVIDO_CONTAGEM"],
        textposition="outside"
        ))
        fig.update_layout(
            barmode="group",
            title="Itens Totais vs Analisados por Quadrante",
            xaxis_title="Quadrante",
            yaxis_title="Quantidade de Itens",
            template="plotly_white"
        )

        
        fig_2 = px.pie(
        df_Quadra_Final,
        names=f"{tipe}",
        values="Vlr Recuperado_x",
        hole=0.4,
        title=f"Valor Recuperado por {tipe}"
    )

        fig_2.update_traces(textinfo="percent+label")
        col_1,col_2 = st.columns([0.50,0.50])
        with col_2:
            st.plotly_chart(fig_2, use_container_width=True)
        with col_1:
            st.plotly_chart(fig, use_container_width=True)

        if tipe == "DESCRDIVISAO_x":
            tipo_inverso = "quadrante"
        else:
            tipo_inverso = "DESCRDIVISAO_x"
        
        if selecao:

            selected_rows = selecao["selection"]["rows"]
            
            if selected_rows:
            
                selected_index = selected_rows[0]
                nome_quadrante = df_Quadra_Final.iloc[selected_index][f"{tipe}"]
                df_geral_filtrado = df_final.loc[df_final[f"{tipe}"] == nome_quadrante ]
                df_loja = df_geral_filtrado.groupby(["IDEMPRESA"]).agg({"resolvido":"count",}).reset_index()
                
                sele_lojas = st.selectbox(F"escolha a loja {tipe}",df_geral_filtrado["IDEMPRESA"].unique())
                sele_lojas
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

                # --- 3️⃣ Plotly vertical bar chart ---
                fig_final_loja = px.bar(
                    df_melted,
                    x="IDEMPRESA",
                    y="Quantidade",
                    color="Tipo",
                    barmode="group",
                    text_auto=True,
                    title="Comparativo de Itens Resolvidos x Totais por Loja",
                    color_discrete_map={
                        "TOTAL_ITENS": "#1f77b4",  # azul
                        "RESOLVIDOS": "#2ca02c"   # verde
                    }
                )

                fig_final_loja.update_layout(
                    xaxis_title="IDEMPRESA (Loja)",
                    yaxis_title="Quantidade de Itens",
                    legend_title="Tipo",
                    bargap=0.15,
                
                )

                # --- 4️⃣ Display ---
                fig_final_loja
                # ou em Streamlit:
                # st.plotly_chart(fig, use_container_width=True)
            
else:
    
    if data_escolhida:
        analista = st.selectbox("Analistas:", list(ANALISTAS.keys()))
        data_path = os.path.join(BASE_DIR, data_escolhida)

        arquivos = listar_arquivos(data_path)
        arquivos_filtrados = filtrar_planilhas_por_analista(arquivos, analista)

        if not arquivos_filtrados:
            st.warning("Nenhuma planilha encontrada para este analista.")
        else:
            st.subheader(f"")    
                           
    #df_final = pd.read_parquet(rf"C:\Users\FERREIRA\OneDrive\teste leitura\exports\{data_escolhida}\{analista}_consolidado.parquet")
    
    #C:\Users\Ferreira\OneDrive\Analises quadrantes\Lucas  
    url_LUCAS = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main/LUCAS_consolidado.parquet"
    df_LUCAS = pd.read_parquet(url_LUCAS)
    url_gabriel = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main/GABRIEL_consolidado.parquet"
    df_Gabriel = pd.read_parquet(url_gabriel)
    url_bruno = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main/BRUNO_consolidado.parquet"
    df_Bruno = pd.read_parquet(url_bruno)

    df_final = pd.concat([df_LUCAS,df_Gabriel,df_Bruno],axis=0) 
        
    from typing import Generator, Sequence
    df_final["Vlr Recuperado"] = pd.to_numeric(df_final["Vlr Recuperado"], errors="coerce").astype("Int64")

    df_final["quadrante"] = df_final["quadrante"].replace(quadrante_nomes)

    data_escolhida_dt = pd.to_datetime(data_escolhida)
    dia_anterior_str = dia_anterior.strftime("%Y-%m-%d")

    #df_final_2 = pd.read_parquet(rf"C:\Users\FERREIRA\OneDrive\teste leitura\exports\{dia_anterior_str}\{analista}_consolidado.parquet")
    
    url_LUCAS = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main/LUCAS_consolidado.parquet"
    df_Lucas_2 = pd.read_parquet(url_LUCAS)
    url_gabriel = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main/GABRIEL_consolidado.parquet"
    df_Gabriel_2 = pd.read_parquet(url_gabriel)
    url_bruno = "https://raw.githubusercontent.com/GFONTINELES/analise-quadrantes-app/main/BRUNO_consolidado.parquet"
    df_Bruno_2 = pd.read_parquet(url_bruno)

    df_final_2 = pd.concat([df_Lucas_2,df_Gabriel_2,df_Bruno_2],axis=0) 
    df_final_2["quadrante"] = df_final_2["quadrante"].replace(quadrante_nomes)

    loja_map = {
        1: "01 - Irmã Dulce",
        2: "02 - Lourival Parente",
        3: "03 - Porto Alegre",
        4: "04 - Água Mineral",
        5: "05 - CD Chapadinha ",
        6: "06 - Renascença",
        7: "07 - Parque Piauí",
        8: "08 - São Joaquim",
        9: "09 - Mocambinho",
        10: "10 - Morada do Sol",
        11: "11 - Dirceu",
        12: "12 - Centro",
        13: "13 - CD frios",
        14: "14 - Água Branca",
        16: "16 - Emp. Dom Severino",
        17: "17 - Cristo Rei",
        18: "18 - Emp. Mocambinho",
        22: "22 - Noé Mendes",
        23: "23 - Kennedy",
        101 : "101 - CD Areias"

        }
    tipo = ["Vlr Recuperado"]
    modos = ['sum','count']


    for modo in modos:
        for t in tipo:
            df_final["Vlr Recuperado"] = pd.to_numeric(df_final["Vlr Recuperado"], errors="coerce")

        # Filter out rows where value is missing or zero if you want to exclude them from count
            df_valid = df_final[df_final["Vlr Recuperado"].notna()]
            
            if modo == "sum":
                agg_func = "sum"
            elif modo == "count":
                # Count only where > 0
                agg_func = lambda x: (x > 0).sum()

            df_Pivot_qtd_GROUP = (
                df_valid
                .groupby(["IDEMPRESA","quadrante","Areas"])
                .agg({t: agg_func})
                .reset_index()
            )
            
            df_Pivot_qtd_GROUP['IDEMPRESA'] = df_Pivot_qtd_GROUP['IDEMPRESA'].replace(loja_map)
            
            df_Pivot_qtd = df_Pivot_qtd_GROUP.pivot_table(columns='IDEMPRESA',index='Areas',values=f'{t}',aggfunc="sum")
                    
            df_Pivot_qtd_GROU_GRAPH = df_Pivot_qtd_GROUP.groupby(["IDEMPRESA","Areas"]).agg({f'{t}': f'{modo}'}).reset_index()
            df_Pivot_qtd_GROU_GRAPH_Area = df_Pivot_qtd_GROU_GRAPH.copy()
            
            df_Pivot_qtd_GROU_GRAPH_Area = (
                df_Pivot_qtd_GROU_GRAPH_Area[df_Pivot_qtd_GROU_GRAPH_Area["Areas"].astype(str).str.strip() != ""]
                .dropna(subset=["Areas"])
            )
    
            
            fig_2 = px.pie(
                df_Pivot_qtd_GROU_GRAPH_Area,
                names='Areas',
                values=f'{t}',
                hole=0.4,title=f"Valor Recuperado por Area"
            )

            fig_2.update_traces(textinfo="percent+label")
            
            fig_LOJA = go.Figure()
            fig_LOJA.add_trace(go.Bar(
                    x=df_Pivot_qtd_GROUP.groupby(["IDEMPRESA"]).agg({f'{t}': f'{modo}'}).reset_index()[f"IDEMPRESA"],
                    y=df_Pivot_qtd_GROUP.groupby(["IDEMPRESA"]).agg({f'{t}': "sum"}).reset_index()[f'{t}'],
                    name=f"Total Itens {modo}",
                    marker_color="royalblue", text=df_Pivot_qtd_GROUP.groupby(["IDEMPRESA"]).agg({f'{t}': "sum"}).reset_index()[f'{t}'],
                textposition="outside"
                ))
            fig_LOJA.update_layout(
        height=600  # increase this number to make it taller (default ~450)
    )

            col_Fig_1, col_Fig_2 = st.columns([0.70,.30])
            
            with col_Fig_1:
                st.write(f"")
                st.write(f"")
                st.write(f"")
                st.write(f"")
                st.write(f"")
                st.write(f"")
                
                if modo == "sum":
                    st.header("Por Loja Valor Recuperado")
                elif modo == "count":
                    st.header("Por Loja Quantidade Recuperado")

                # Count only where > 0
                styled_df = df_Pivot_qtd
                
            
            df_Pivot_qtd

            with col_Fig_2:

                st.plotly_chart(fig_2, key=f"pie_{t}_{modo}")

            
            fig_LOJA
