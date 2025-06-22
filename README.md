# TP2_IBD

Este é o repositório do trabalho prático 2 da matéria **Introdução a Banco de Dados** - **UFMG - 2025/01** - Professor **Clodoveu Davis**.

**Grupo:**  
- Eduardo Birchal - 2024023970  
- Enzo de Souza Braz - 2024099062  
- Gabriel Guimarães dos Santos Ricardo - 2024024062  
- Gabriel Lucas Martins - 2023034900  
- João Pedro Moreira Smolinski - 2024023996  

---

## Execução dos scripts
Para executar os scripts, você precisa ter os CSVs específicos e rodar via linha de comando.  

### Scripts e seus requisitos:  

#### `filtrar_mort_munic_ufs.py`  
**Função:**  
Filtrar dados usando regras de colunas obrigatórias definidas pelo grupo.  

**CSVs necessários:**  
1. `Mortalidade_XXXX.csv` (dados abertos do Ministério da Saúde)  
2. `ufs.csv` (dados abertos do IBGE customizado)  
3. `municipios.csv` (dados abertos do IBGE customizado)  

---

#### `filtrar_zika_dengue.py`  
**Função:**  
Aplica filtragem semelhante ao script anterior, mas para dados de zika/dengue.  

**CSVs necessários:**  
1. `zika_XXXX.csv` (dados abertos do Ministério da Saúde)  
2. `dengue_XXXX.csv` (dados abertos do Ministério da Saúde)  

---

#### `juntar_zika_dengue.py`  
**Função:**  
Unificar CSV anuais de zika e dengue em datasets consolidados.  

**CSVs necessários:**  
1. `zika_XXXX.csv` (dados abertos do Ministério da Saúde)  
2. `dengue_XXXX.csv` (dados abertos do Ministério da Saúde)  

---

#### `relatorios_colunas.py`  
**Função:**  
Gera relatório com tipos de dados e estrutura das colunas para auxiliar no planejamento do banco.  

**CSVs necessários:**  
Todos os mencionados nos scripts anteriores.  

---

#### `sql_create.py`  
**Função:**  
Gera script SQL para criação do banco de dados e tabelas.  

**CSV necessário:**  
`dicionario.csv` (dicionário da modelagem do banco de dados)  