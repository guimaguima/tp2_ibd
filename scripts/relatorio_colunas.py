import os
import pandas as pd
from collections import Counter, defaultdict

PASTA = "../data/"
ARQUIVOS = [f for f in os.listdir(PASTA) if f.endswith(".csv")]

CHUNK_SIZE = 10000

def tipo_simples(valor):
    if pd.isna(valor) or str(valor).strip().lower() in {'nan', 'null', 'n/a', ''}:
        return 'nulo'
    valor = str(valor).strip()
    if valor.lower() in ['true', 'false']:
        return 'bool'
    try:
        int(valor)
        return 'int'
    except:
        try:
            float(valor)
            return 'str' 
        except:
            return 'str'

relatorio = []

for nome_arquivo in ARQUIVOS:
    caminho = os.path.join(PASTA, nome_arquivo)
    print(f"Lendo {nome_arquivo}...")

    contadores = defaultdict(Counter)
    exemplos = defaultdict(lambda: defaultdict(list))
    total_linhas = 0

    try:
        for chunk in pd.read_csv(
            caminho,
            chunksize=CHUNK_SIZE,
            dtype=str,
            low_memory=False,
            sep=";"
        ):
            total_linhas += len(chunk)

            for col in chunk.columns:
                tipos = chunk[col].map(tipo_simples)

                contadores[col].update(tipos)

                for tipo_nome, valor in zip(tipos, chunk[col]):
                    if len(exemplos[col][tipo_nome]) < 3:
                        exemplos[col][tipo_nome].append(str(valor))
    except Exception as e:
        relatorio.append(f"[ERRO] Falha ao ler {nome_arquivo}: {e}\n")
        continue

    relatorio.append(f"===== {nome_arquivo} ({total_linhas} linhas) =====")
    for col in contadores:
        if len(contadores[col]) > 1:
            relatorio.append(f"Coluna: {col}")
            for tipo, count in contadores[col].items():
                relatorio.append(f"   {tipo}: {count}")
                for ex in exemplos[col][tipo]:
                    relatorio.append(f"      exemplo: {ex}")
    relatorio.append("")

with open("relatorio_tipos.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(relatorio))

print("Relatório gerado: relatorio_tipos.txt")
