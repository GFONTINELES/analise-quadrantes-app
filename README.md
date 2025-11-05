# 📊 Análise de Quadrantes - Streamlit

Este projeto fornece uma aplicação interativa desenvolvida em **Python** e **Streamlit**, criada para auxiliar gestores na análise e visualização de movimentações de estoque, desempenho e quadrantes de produtos.  
A aplicação permite consultar dados de múltiplos analistas e gerar gráficos dinâmicos diretamente pelo navegador — inclusive pelo **celular**.

## 🚀 Funcionalidades Principais

- Leitura automática de arquivos `.parquet` hospedados no GitHub (sem necessidade de acesso local);
- Filtros interativos por analista, período e área;
- Indicadores visuais e dashboards gerados com **Plotly**;
- Exportação e atualização automática de dados;
- Interface responsiva e intuitiva (funciona também em dispositivos móveis);
- Integração com **Streamlit Cloud**, possibilitando acesso via link público.

🌐 Publicação no Streamlit Cloud

Faça login em https://streamlit.io/cloud;

Conecte sua conta do GitHub;

Clique em "New app" → selecione este repositório → branch principal (main);

Defina o arquivo principal como:

analise_quadrantes_Geral.py

Clique em Deploy.

O Streamlit gerará um link público como este:

https://analise-quadrantes-app.streamlit.app

