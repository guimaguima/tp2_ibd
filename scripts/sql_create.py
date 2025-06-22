import psycopg2
import csv
import re
import unicodedata

DB_HOST = 'localhost'
DB_USER = 'postgres'
DB_PASSWORD = 'sua_senha'
DB_NAME = 'ibd_db'

def normalize_name(name):
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
    return re.sub(r'[^a-zA-Z0-9]', '_', name).lower()

RESERVED_WORDS = {'desc', 'group', 'order', 'table', 'select', 'where', 'check'}

tables = {}
current_table = None

with open('dicionario.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        relation = row['Relações'].strip()
        
        if relation:
            current_table = normalize_name(relation)
            if current_table not in tables:
                tables[current_table] = []
        
        if current_table:
            tables[current_table].append(row)

table_order = [
    'unidade_federativa',
    'municipios',
    'ubs',
    'escolaridade',
    'raca',
    'tipo_notificacao',
    'tipo_infectado',
    'notificacao_de_infectados'
]

def map_data_type(data_type_str):
    if '(' in data_type_str:
        base_type = data_type_str.split('(')[0].strip().upper()
        size = data_type_str.split('(')[1].split(')')[0].strip()
    else:
        base_type = data_type_str.strip().upper()
        size = None
    
    if base_type == 'NUMBER':
        if size:
            return f'NUMERIC({size})'
        return 'INTEGER'
    elif base_type == 'CHAR':
        return f'CHAR({size})' if size else 'CHAR'
    elif base_type == 'VARCHAR':
        return f'VARCHAR({size})' if size else 'VARCHAR'
    elif base_type == 'FLOAT':
        return 'REAL'
    elif base_type == 'INTEGER':
        return 'INTEGER'
    elif base_type == 'BOOLEAN':
        return 'BOOLEAN'
    elif base_type == 'DATE':
        return 'DATE'
    else:
        return data_type_str

with open('schema.sql', 'w', encoding='utf-8') as schema_file:
    schema_file.write(f"DROP DATABASE IF EXISTS {DB_NAME};\n")
    schema_file.write(f"CREATE DATABASE {DB_NAME};\n\n")
    schema_file.write(f"\\connect {DB_NAME};\n\n")

    for table_name in table_order:
        if table_name not in tables:
            continue
            
        columns = []
        pks = []
        fks = []
        checks = []
        value_constraints = []
        
        for row in tables[table_name]:
            col_name = row['Atributo'].strip()
            null_allowed = row['Nulo'] == 'S'
            unique = row['Unico'] == 'S'
            
            if col_name.lower() in RESERVED_WORDS:
                col_name_escaped = f'"{col_name}"'
            else:
                col_name_escaped = col_name
            
            pg_type = map_data_type(row['Tipo'])
            nullable = '' if null_allowed else 'NOT NULL'
            unique_clause = ' UNIQUE' if unique else ''
            
            col_def = f"{col_name_escaped} {pg_type}"
            if nullable:
                col_def += f" {nullable}"
            if unique_clause:
                col_def += f" {unique_clause}"
            columns.append(col_def)
            
            if 'PK' in row['Restrições']:
                pks.append(col_name_escaped)
            
            fk_match = re.search(r'FK\((\w+)\)\s*REFERENCES\s*(\w+)\s*\((\w+)\)\s*(ON DELETE \w+)?', 
                                row['Restrições'])
            if fk_match:
                fk_col = fk_match.group(1)
                ref_table = normalize_name(fk_match.group(2))
                ref_col = fk_match.group(3)
                on_delete = fk_match.group(4) or 'ON DELETE RESTRICT'
                
                if fk_col.lower() in RESERVED_WORDS:
                    fk_col = f'"{fk_col}"'
                    
                fks.append(f"FOREIGN KEY ({fk_col}) REFERENCES {ref_table}({ref_col}) {on_delete}")
            
            if 'CHECK' in row['Restrições']:
                check_expr = row['Restrições'].replace('CHECK', '').strip()
                
                if table_name == 'notificacao_de_infectados' and row['Atributo'] == 'cs_gestant':
                    check_expr = check_expr.replace('sexo', 'cs_sexo')
                    check_expr = check_expr.replace('YEAR(GETDATE())', 'EXTRACT(YEAR FROM CURRENT_DATE)')
                
                if 'tipo_infec' in check_expr:
                    check_expr = f"((tipo_infec = 1 AND {col_name_escaped} IS NULL) OR (tipo_infec <> 1))"
                
                if check_expr.startswith('(') and check_expr.endswith(')'):
                    check_expr = check_expr[1:-1]
                
                if check_expr.strip():
                    checks.append(f"CHECK ({check_expr})")
            
            allowed_values = row['Valores Permitidos'].strip()
            if allowed_values:
                values = []
                quoted_values = re.findall(r"'(.*?)'", allowed_values)
                digit_values = re.findall(r'\b\d+\b', allowed_values)
                char_values = re.findall(r'\b[A-Za-z]\b', allowed_values)
                
                values = quoted_values + digit_values + char_values
                
                valid_values = []
                for v in values:
                    v = v.strip()
                    if v and v not in valid_values:
                        valid_values.append(v)
                
                if valid_values:
                    if all(v.isdigit() for v in valid_values):
                        formatted_values = ", ".join(valid_values)
                    else:
                        formatted_values = ", ".join(f"'{v}'" for v in valid_values)
                    
                    value_constraints.append(f"CHECK ({col_name_escaped} IN ({formatted_values}))")
        
        create_sql = f"CREATE TABLE {table_name} (\n"
        create_sql += ",\n".join(columns)
        
        if pks:
            create_sql += f",\nPRIMARY KEY ({', '.join(pks)})"
        
        if fks:
            create_sql += ",\n" + ",\n".join(fks)
        
        if checks:
            create_sql += ",\n" + ",\n".join(checks)
        
        if value_constraints:
            create_sql += ",\n" + ",\n".join(value_constraints)
        
        create_sql += "\n);\n"
        
        schema_file.write(create_sql + "\n")

print("Arquivo schema.sql gerado com sucesso!")

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME};")
    cur.execute(f"CREATE DATABASE {DB_NAME};")
    cur.close()
    conn.close()
    
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    conn.autocommit = True  
    cur = conn.cursor()
    
    with open('schema.sql', 'r') as f:
        content = f.read()
        create_table_commands = re.findall(r'CREATE TABLE [^;]*;', content, re.IGNORECASE)
        
        for cmd in create_table_commands:
            try:
                cur.execute(cmd)
                print(f"Tabela criada: {cmd.split()[2]}")
            except Exception as e:
                print(f"Erro ao criar tabela: {cmd.split()[2]}")
                print(f"Detalhes: {str(e)}")
    
    print("Banco de dados criado com sucesso!")
    
except Exception as e:
    print(f"Erro geral: {str(e)}")
finally:
    if 'cur' in locals() and cur:
        cur.close()
    if 'conn' in locals() and conn:
        conn.close()