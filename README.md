# Dados Dengue e Zika

Este repositório foi originalmente feito para o trabalho prático 2 da matéria **Introdução a Banco de Dados** - **UFMG - 2025/01** - Professor **Clodoveu Davis**.
A ideia central é analisar, organizar e montar os dados de notificações de dengue e zika de 2023 até 2025, tudo isto utilizando de um banco de dados em PostgresSQL.

**Grupo:**  
- Eduardo Birchal 
- Enzo de Souza Braz 
- Gabriel Guimarães dos Santos Ricardo
- Gabriel Lucas Martins  
- João Pedro Moreira Smolinski 

---

## Dados
Todos os dados utilizados seguem o que foi descrito pelo pdf do nosso relatório. Pedimos que antes de rodar qualquer código leia-se atentamento o que foi feito. 
Posteriormente, garanta que você tenha todos os arquivos necessários para cada etapa do processo, aqueles que não estão aqui listados podem ser adquirido pelo seguinte link (além das fontes originais - as quais sugerimos que use caso almeje reproduzir nossos resultados): https://drive.google.com/drive/folders/1daqI1kAXDIXUIQwwZUI3Ujqw1q4vJgCt?usp=drive_link;

## Execução dos scripts
Para executar os scripts, você precisa ter os CSVs específicos e rodar via linha de comando.  

### Scripts e seus requisitos:  

#### `filtrar_mort_munic_ufs.py`  
**Função:**  
Filtrar dados usando regras de colunas obrigatórias definidas pelo grupo.  

**CSVs necessários:** 
2. `ufs.csv` (dados abertos do IBGE customizado - Créditos e fonte: https://github.com/kelvins/municipios-brasileiros + Dados adicionados por nós! (Somente no Drive))  
3. `municipios.csv` (dados abertos do IBGE customizado - Créditos e fonte: https://github.com/kelvins/municipios-brasileiros)

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

## Criação do Banco de Dados (bd.py)
Para criar o banco de dados, você precisa dos CSV's na pasta data/:

1. dados_unificados.csv
2. municipios.csv
3. ufs.csv
4. dicionario.csv

Além do arquivo `bd.py` dentro da pastra scripts/

Em seguida execute o script `bd.py`

Um arquivo `schema.sql` vai ser criado com base nas configurações do `dicionario.csv`
Após isso a configuração e população do banco deve começar.

Certifique-se de criar um usuário `db_user` no PostgreSQL que tenha atribuições  de criação de bancos, assim não haverão conflitos durante essa etapa.

A execução do script é demorada e alguns erros são esperados, mas não interrompa a execução, são apenas erros de contenção das tabelas para eliminação de linhas inválidas. Espera-se tempo de execução médio de 1 hora para a inserção de todas as 4.6 milhões de linhas na base de dados.