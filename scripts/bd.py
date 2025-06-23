import psycopg2
import csv
import re
import unicodedata
import json
import os
from psycopg2.extras import execute_values

DB_HOST = 'localhost'
DB_USER = 'db_user'
DB_PASSWORD = '123'
DB_NAME = 'ibd_db'

DADOS_LOOKUP = {
    "escolaridade": {
        "columns": ('id', '"desc"'),
        "data": {
            0: "Analfabeto", 1: "1ª a 4ª série incompleta do EF", 2: "4ª série completa do EF",
            3: "5ª a 8ª série incompleta do EF", 4: "Ensino fundamental completo", 5: "Ensino médio incompleto",
            6: "Ensino médio completo", 7: "Educação superior incompleta", 8: "Educação superior completa",
            9: "Ignorar", 10: "Não se aplica",
        }
    },
    "raca": {
        "columns": ('id', '"desc"'),
        "data": { 1: "Branca", 2: "Preta", 3: "Amarela", 4: "Parda", 5: "Indígena", 9: "Ignorar" }
    },
    "tipo_notificacao": {
        "columns": ('id', 'tipo'),
        "data": { 1: "Negativa", 2: "Individual", 3: "Surto", 4: "Agregado" }
    },
    "tipo_infectado": {
        "columns": ('id', '"desc"'),
        "data": { 1: "dengue", 2: "zika" }
    },
}

MAPEAMENTO_COLUNAS_CSV = {
    "notificacao_de_infectados": {
        'sexo': 'cs_sexo',
        'gestante': 'cs_gestant'
    },
    "unidade_federativa": {
        'codigo': 'cod_uf',
        'latitude': 'lat_uf',
        'longitude': 'long_uf',
        'populacao': 'populacao'
    },
    "municipios": {
        'codigo_ibge': 'cod_mun',
        'nome': 'nome_mun',
        'latitude': 'lat_mun',
        'longitude': 'long_mun',
        'fuso_horario': 'fuso',
        'codigo_uf': 'cod_uf'
    }
}

ARQUIVOS_CSV_PARA_INSERCAO_AUTO = {
    "unidade_federativa": "../data/ufs_filtrado.csv",
    "municipios": "../data/municipios.csv",
    "notificacao_de_infectados": "../data/dados_unificados.csv"
}

ERROS_TOLERADOS = (
    psycopg2.errors.NotNullViolation,
    psycopg2.errors.ForeignKeyViolation,
    psycopg2.errors.NumericValueOutOfRange,
)

CONSTRAINTS_TOLERADAS = [
    'chk_notificacao_de_infectados_alrm_abdom_logic',
]


def is_number(s):
    """Verifica se uma string pode ser convertida para um número."""
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False

def normalize_name(name):
    """Normaliza nomes de tabelas, removendo acentos e caracteres especiais."""
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'[^a-zA-Z0-9]', '_', name).lower()

RESERVED_WORDS = {'desc', 'group', 'order', 'table', 'select', 'where', 'check'}


def criar_schema_e_tabelas():
    """
    Lê o ../data/dicionario.csv, gera um arquivo schema.sql e o executa para criar o
    banco de dados e as tabelas, incluindo restrições nomeadas para depuração.
    Retorna True se bem-sucedido, False caso contrário.
    """
    tables = {}
    current_table = None

    try:
        with open('../data/dicionario.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                relation = row['Relações'].strip()
                if relation:
                    current_table = normalize_name(relation)
                    if current_table not in tables:
                        tables[current_table] = []
                if current_table:
                    tables[current_table].append(row)
    except FileNotFoundError:
        print("Erro: O arquivo '../data/dicionario.csv' não foi encontrado.")
        return False

    table_order = ['unidade_federativa', 'municipios', 'escolaridade', 'raca', 'tipo_notificacao', 'tipo_infectado', 'notificacao_de_infectados']

    def map_data_type(data_type_str):
        """Converte tipos de dados do dicionário para tipos SQL do PostgreSQL."""
        if '(' in data_type_str:
            base_type, size = data_type_str.split('(')[0].strip().upper(), data_type_str.split('(')[1].split(')')[0].strip()
        else:
            base_type, size = data_type_str.strip().upper(), None
        type_map = {'NUMBER': f'NUMERIC({size})' if size else 'INTEGER', 'CHAR': f'CHAR({size})', 'VARCHAR': f'VARCHAR({size})', 'FLOAT': 'REAL'}
        return type_map.get(base_type, base_type)

    with open('schema.sql', 'w', encoding='utf-8') as schema_file:
        schema_file.write(f"-- Schema gerado automaticamente a partir do ../data/dicionario.csv\n\n")
        for table_name in table_order:
            if table_name not in tables: continue
            columns, pks, fks, checks, value_constraints = [], [], [], [], []
            for row in tables[table_name]:
                col_name = row['Atributo'].strip()
                col_name_escaped = f'"{col_name}"' if col_name.lower() in RESERVED_WORDS else col_name
                not_null_clause = '' if row['Nulo'] == 'S' else ' NOT NULL'
                unique_clause = ' UNIQUE' if row['Unico'] == 'S' else ''
                columns.append(f"{col_name_escaped} {map_data_type(row['Tipo'])}{not_null_clause}{unique_clause}")
                
                if 'PK' in row['Restrições']: pks.append(col_name_escaped)
                fk_match = re.search(r'FK\((\w+)\).*REFERENCES\s*(\w+)\s*\((\w+)\)', row['Restrições'])
                if fk_match:
                    fk_col, ref_table, ref_col = fk_match.groups()
                    on_delete = re.search(r'ON DELETE (\w+)', row['Restrições'])
                    on_delete_clause = on_delete.group(0) if on_delete else 'ON DELETE RESTRICT'
                    fks.append(f"FOREIGN KEY ({fk_col}) REFERENCES {normalize_name(ref_table)}({ref_col}) {on_delete_clause}")
                
                if 'CHECK' in row['Restrições']:
                    check_expr_original = row['Restrições'].replace('CHECK', '').strip()
                    
                    if 'tipo_infec' in check_expr_original:
                        check_expr = f"((tipo_infec = 1 AND {col_name_escaped} IS NOT NULL) OR (tipo_infec <> 1))"
                    else:
                        check_expr = check_expr_original
                        if table_name == 'notificacao_de_infectados':
                            check_expr = check_expr.replace('gestante', 'cs_gestant')
                            check_expr = check_expr.replace('sexo', 'cs_sexo')
                            check_expr = check_expr.replace('YEAR(GETDATE())', 'EXTRACT(YEAR FROM CURRENT_DATE)')

                    constraint_name = f"chk_{table_name}_{col_name}_logic"
                    if check_expr: checks.append(f"CONSTRAINT {constraint_name} CHECK ({check_expr})")
                
                allowed_values = row['Valores Permitidos'].strip()
                if allowed_values:
                    values = re.findall(r"'([^']*)'|\b\w+\b", allowed_values)
                    valid_values = [v for v in values if v]
                    
                    if valid_values:
                        formatted_values = ", ".join(f"'{v}'" if not v.isdigit() else v for v in valid_values)
                        constraint_name = f"chk_{table_name}_{col_name}_values"
                        value_constraints.append(f"CONSTRAINT {constraint_name} CHECK ({col_name_escaped} IN ({formatted_values}))")

            create_sql = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(columns)
            if pks: create_sql += f",\n  PRIMARY KEY ({', '.join(pks)})"
            if fks: create_sql += ",\n  " + ",\n  ".join(fks)
            if checks: create_sql += ",\n  " + ",\n  ".join(checks)
            if value_constraints: create_sql += ",\n  " + ",\n  ".join(value_constraints)
            create_sql += "\n);\n\n"
            schema_file.write(create_sql)
    print("Arquivo schema.sql gerado com sucesso!")

    try:
        conn = psycopg2.connect(f"host='{DB_HOST}' user='{DB_USER}' password='{DB_PASSWORD}' dbname='postgres'")
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME};")
        cur.execute(f"CREATE DATABASE {DB_NAME};")
        cur.close(); conn.close()
        
        conn = psycopg2.connect(f"host='{DB_HOST}' user='{DB_USER}' password='{DB_PASSWORD}' dbname='{DB_NAME}'")
        cur = conn.cursor()
        with open('schema.sql', 'r') as f:
            cur.execute(f.read())
        conn.commit()
        print("Banco de dados e tabelas criados com sucesso!")
        return True
    except Exception as e:
        print(f"Erro geral durante a execução do schema: {e}")
        return False
    finally:
        if 'conn' in locals() and not conn.closed: conn.close()

def inserir_dados_lookup():
    """
    Insere os dados das tabelas de lookup (domínio) definidos no dicionário DADOS_LOOKUP.
    """
    conn_string = f"host='{DB_HOST}' dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}'"
    print("\nIniciando a inserção dos dados de lookup...")
    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                for table_name, content in DADOS_LOOKUP.items():
                    columns = content["columns"]
                    data_dict = content["data"]
                    
                    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
                    data_to_insert = list(data_dict.items())
                    
                    execute_values(cur, sql, data_to_insert)
                    print(f"-> {cur.rowcount} linhas inseridas na tabela '{table_name}'.")
        print("Dados de lookup inseridos com sucesso!")
    except Exception as e:
        print(f"Ocorreu um erro ao inserir os dados de lookup: {e}")

def get_tipos_de_coluna_da_tabela(cursor, table_name):
    """
    Busca e retorna um dicionário com os nomes e tipos das colunas 
    de uma tabela diretamente do banco de dados (information_schema).
    """
    sql_query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s;"
    cursor.execute(sql_query, (table_name,))
    return {row[0]: row[1] for row in cursor.fetchall()}

def mapear_e_filtrar_colunas(table_name, header_from_csv, actual_db_columns):
    """
    Mapeia nomes de colunas do CSV para o DB e filtra, mantendo apenas as que existem na tabela.
    Retorna duas listas: os nomes das colunas do CSV a serem usadas e os nomes das colunas do DB correspondentes.
    """
    db_columns_map = MAPEAMENTO_COLUNAS_CSV.get(table_name, {})
    
    csv_cols_to_use = []
    db_cols_to_use = []

    for csv_col in header_from_csv:
        db_col = db_columns_map.get(csv_col.lower(), csv_col.lower())
        
        if db_col in actual_db_columns:
            csv_cols_to_use.append(csv_col)
            db_cols_to_use.append(db_col)
            
    return csv_cols_to_use, db_cols_to_use

def transformar_linha(table_name, row, db_column_types, lookup_data={}):
    """
    Aplica transformações de dados a uma linha, com base na tabela de destino e nos tipos de coluna do DB.
    """
    csv_to_db_map = MAPEAMENTO_COLUNAS_CSV.get(table_name, {})

    for csv_col, value in row.items():
        if value is None or value == '':
            row[csv_col] = None
            continue

        db_col = csv_to_db_map.get(csv_col.lower(), csv_col.lower())
        col_type = db_column_types.get(db_col)

        if isinstance(value, str):
            if col_type == 'real':
                row[csv_col] = value.replace(',', '.')
            elif col_type == 'boolean' and is_number(value):
                row[csv_col] = (int(float(value)) > 1)

    if table_name == 'notificacao_de_infectados':
        if tipo_infec_str := row.get('tipo_infec', ''):
            if isinstance(tipo_infec_str, str):
                 row['tipo_infec'] = 1 if tipo_infec_str.lower() == 'dengue' else 2 if tipo_infec_str.lower() == 'zika' else None
        
        municipios_map = lookup_data.get('municipios_map', {})
        for col_name in ['cod_mun_infec', 'cod_mun_res']:
            cod_mun_6_digitos = str(row.get(col_name))[:6] if row.get(col_name) else None
            
            if cod_mun_6_digitos:
                cod_mun_7_digitos = municipios_map.get(cod_mun_6_digitos)
                if cod_mun_7_digitos:
                    row[col_name] = cod_mun_7_digitos
                else:
                    print(f"AVISO: Não foi encontrado um município correspondente para o {col_name}: {cod_mun_6_digitos}. Esta linha provavelmente falhará.")
                    row[col_name] = None

    return row

def inserir_dados_em_tabela(table_name, csv_path):
    """
    Função genérica para carregar dados de um CSV para uma tabela específica.
    """
    conn_string = f"host='{DB_HOST}' dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}'"
    batch_size = 20000
    total_inserido = 0
    total_ignorado = 0

    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cur:
                
                lookup_data = {}
                if table_name == 'notificacao_de_infectados':
                    print("Pré-carregando códigos de municípios para correspondência...")
                    cur.execute("SELECT cod_mun FROM municipios;")
                    municipios_map = {str(row[0])[:6]: row[0] for row in cur.fetchall()}
                    lookup_data['municipios_map'] = municipios_map
                    print(f"-> {len(municipios_map)} códigos de municípios carregados.")

                db_column_types = get_tipos_de_coluna_da_tabela(cur, table_name)
                actual_db_columns = list(db_column_types.keys())
                
                with open(csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    header_from_csv = reader.fieldnames
                    
                    csv_cols_to_use, db_cols_to_use = mapear_e_filtrar_colunas(table_name, header_from_csv, actual_db_columns)

                    if not db_cols_to_use:
                        print(f"AVISO: Nenhuma coluna do arquivo '{csv_path}' corresponde às colunas da tabela '{table_name}'. Inserção pulada.")
                        return

                    print(f"\nIniciando a inserção em lotes para a tabela '{table_name}'...")
                    print(f"Colunas do CSV a serem inseridas: {', '.join(csv_cols_to_use)}")
                    print(f"Mapeadas para as colunas do DB: {', '.join(db_cols_to_use)}")

                    sql_insert = f"INSERT INTO {table_name} ({', '.join(db_cols_to_use)}) VALUES %s"
                    
                    lote, lote_original = [], []
                    for i, row in enumerate(reader, 1):
                        lote_original.append(dict(row))
                        
                        transformed_row = transformar_linha(table_name, row, db_column_types, lookup_data)
                        
                        lote.append(tuple(transformed_row[col] for col in csv_cols_to_use))
                        
                        if i % batch_size == 0:
                            try:
                                cur.execute("SAVEPOINT batch_savepoint;")
                                execute_values(cur, sql_insert, lote)
                                cur.execute("RELEASE SAVEPOINT batch_savepoint;")
                                total_inserido += len(lote)
                                print(f"{total_inserido} linhas inseridas...\n")
                            except psycopg2.Error as e:
                                cur.execute("ROLLBACK TO SAVEPOINT batch_savepoint;")
                                print(f"\nERRO no lote que termina na linha {i}. Verificando linha por linha...")
                                linhas_inseridas, linhas_ignoradas = lidar_com_lote_com_erro(cur, table_name, lote, lote_original, db_cols_to_use, i - batch_size, conn)
                                if linhas_inseridas == -1: 
                                    conn.rollback()
                                    return
                                total_inserido += linhas_inseridas
                                total_ignorado += linhas_ignoradas
                            lote, lote_original = [], []
                    
                    if lote:
                        try:
                            cur.execute("SAVEPOINT batch_savepoint;")
                            execute_values(cur, sql_insert, lote)
                            cur.execute("RELEASE SAVEPOINT batch_savepoint;")
                            total_inserido += len(lote)
                        except psycopg2.Error as e:
                            cur.execute("ROLLBACK TO SAVEPOINT batch_savepoint;")
                            print(f"\nERRO no último lote. Verificando linha por linha...")
                            linhas_inseridas, linhas_ignoradas = lidar_com_lote_com_erro(cur, table_name, lote, lote_original, db_cols_to_use, total_inserido, conn)
                            if linhas_inseridas == -1:
                                conn.rollback()
                                return
                            total_inserido += linhas_inseridas
                            total_ignorado += linhas_ignoradas
            
            print("\n\n----------------------------------------------------")
            print(f"Tabela '{table_name}' inserida com sucesso! {total_inserido}/{(total_inserido + total_ignorado)} linhas inseridas ({total_ignorado} ignoradas).")
            print("----------------------------------------------------\n\n")

    except FileNotFoundError:
        print(f"\nERRO: O arquivo '{csv_path}' não foi encontrado.")
    except Exception as e:
        print(f"\nOcorreu um erro inesperado: {e}")

def lidar_com_lote_com_erro(cursor, table_name, lote_transformado, lote_original, db_cols, offset, conexao):
    """
    Recebe um lote que falhou, ignora linhas com erros tolerados usando savepoints,
    e para em outros erros. Retorna uma tupla (linhas_inseridas, linhas_ignoradas).
    Em caso de erro fatal, retorna (-1, 0).
    """
    single_insert_sql = f"INSERT INTO {table_name} ({', '.join(db_cols)}) VALUES ({', '.join(['%s'] * len(db_cols))})"
    linhas_inseridas = 0
    linhas_ignoradas = 0

    for idx, data_tuple in enumerate(lote_transformado):
        linha_csv = offset + idx + 2
        try:
            cursor.execute("SAVEPOINT single_row_savepoint;")
            cursor.execute(single_insert_sql, data_tuple)
            cursor.execute("RELEASE SAVEPOINT single_row_savepoint;")
            linhas_inseridas += 1
        except psycopg2.Error as e:
            cursor.execute("ROLLBACK TO SAVEPOINT single_row_savepoint;")
            
            is_tolerable_type = isinstance(e, ERROS_TOLERADOS)
            is_tolerable_constraint = (
                isinstance(e, psycopg2.errors.CheckViolation) and
                hasattr(e.diag, 'constraint_name') and
                e.diag.constraint_name in CONSTRAINTS_TOLERADAS
            )
            
            if is_tolerable_constraint or is_tolerable_type:
                linhas_ignoradas += 1
                constraint_info = f" (Restrição: {e.diag.constraint_name})" if hasattr(e.diag, 'constraint_name') and e.diag.constraint_name else ''
                print(f"AVISO: Linha {linha_csv} ignorada devido a um erro tolerado ({type(e).__name__}){constraint_info}.")
                continue 
            else:
                print("\n--- ERRO CRÍTICO ENCONTRADO ---")
                print(f"Erro na linha {linha_csv} do arquivo CSV.")
                if e.diag and e.diag.constraint_name:
                    print(f"Nome da Restrição: {e.diag.constraint_name}")
                print(f"Mensagem do Banco de Dados: {e.pgerror.strip()}")
                print("\nDados originais da linha que causou o erro:")
                print(json.dumps(lote_original[idx], indent=2))
                print("\nScript interrompido. Corrija o dado no arquivo CSV e tente novamente.")
                return -1, 0

    return linhas_inseridas, linhas_ignoradas


def loop_insercao_automatica():
    """
    Itera sobre o dicionário ARQUIVOS_CSV_PARA_INSERCAO_AUTO e insere os dados
    de cada arquivo na tabela correspondente, procurando os arquivos na mesma
    pasta do script.
    """
    print("\nIniciando a inserção automática de dados dos arquivos CSV...")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    for table_name, csv_filename in ARQUIVOS_CSV_PARA_INSERCAO_AUTO.items():
        full_csv_path = os.path.join(script_dir, csv_filename)
        inserir_dados_em_tabela(table_name, full_csv_path)
        
    print("\nInserção automática de dados finalizada.")


if __name__ == "__main__":
    if criar_schema_e_tabelas():
        inserir_dados_lookup()
        loop_insercao_automatica()
