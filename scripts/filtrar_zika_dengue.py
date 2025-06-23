import pandas as pd

PASTA = "../data/"
ARQUIVOS = [f for f in os.listdir(PASTA) if f.endswith(".csv")]
CHUNKSIZE = 100000

log_remocoes = {}

def filtro_dengue(df):
    obrigatorias = [
        "ID_REGIONA", "ID_UNIDADE", "ANO_NASC", "CS_SEXO", "CS_GESTANT", "CS_RACA",
        "CS_ESCOL_N", "ID_MN_RESI", "DT_INVEST", "DT_ENCERRA", "CLASSI_FIN",
        "FEBRE", "MIALGIA", "CEFALEIA", "EXANTEMA", "VOMITO", "NAUSEA",
        "DOR_COSTAS", "CONJUNTVIT", "ARTRITE", "ARTRALGIA", "PETEQUIA_N",
        "LEUCOPENIA", "LACO", "DOR_RETRO", "DIABETES", "HEMATOLOG", "HEPATOPAT",
        "RENAL", "HIPERTENSA", "ACIDO_PEPT", "AUTO_IMUNE", "DT_DIGITA"
    ]
    if linha["TPAUTOCTO"] != 1:
        return False
    mask = df[obrigatorias].notna().all(axis=1)
    return mask

def filtro_zika(df):
    obrigatorias = [
        "ID_REGIONA", "ANO_NASC", "CS_ESCOL_N", "ID_MN_RESI",
        "DT_INVEST", "DT_ENCERRA", "CLASSI_FIN", "SG_UF", "DT_DIGITA"
    ]
    mask = df[obrigatorias].notna().all(axis=1)
    return mask

for nome_arquivo in ARQUIVOS:
    caminho = os.path.join(PASTA, nome_arquivo)

    if nome_arquivo.startswith("DENGBR"):
        tipo = "dengue"
        filtro = filtro_dengue
    elif nome_arquivo.startswith("ZIKABR"):
        tipo = "zika"
        filtro = filtro_zika
    else:
        print(f"Ignorando {nome_arquivo} (prefixo desconhecido)")
        continue

    print(f"Processando {nome_arquivo} ({tipo})")

    try:
        colunas_originais = pd.read_csv(caminho, nrows=0).columns.tolist()
    except Exception as e:
        print(f"Erro ao ler cabeçalho de {nome_arquivo}: {e}")
        continue

    original_total = 0
    filtrado_total = 0
    saida = os.path.join(PASTA, nome_arquivo.replace(".csv", "_filtrado.csv"))
    header_escrito = False

    try:
        for i, chunk in enumerate(pd.read_csv(caminho, chunksize=CHUNKSIZE, dtype={'ID_OCUPA_N': str}, low_memory=False)):

            # Garante todas as colunas presentes
            for col in colunas_originais:
                if col not in chunk.columns:
                    chunk[col] = pd.NA
            chunk = chunk[colunas_originais]

            original_chunk = len(chunk)
            mask = filtro(chunk)
            chunk_filtrado = chunk[mask]
            filtrado_chunk = len(chunk_filtrado)

            original_total += original_chunk
            filtrado_total += filtrado_chunk

            chunk_filtrado.to_csv(saida, mode='a', index=False, header=not header_escrito)
            if not header_escrito:
                header_escrito = True

            print(f"Chunk {i+1} processado: {original_chunk} linhas, {filtrado_chunk} mantidas")

    except Exception as e:
        print(f"Erro no processamento de {nome_arquivo}: {e}")
        continue

    removidas = original_total - filtrado_total
    log_remocoes[nome_arquivo] = {
        "tipo": tipo,
        "original": original_total,
        "filtrado": filtrado_total,
        "removidas": removidas
    }

print("\nRESUMO DO FILTRO:")
print("{:<25} {:<8} {:<10} {:<10} {:<10}".format("Arquivo", "Tipo", "Original", "Mantidas", "Removidas"))
for nome, info in log_remocoes.items():
    print("{:<25} {:<8} {:<10} {:<10} {:<10}".format(
        nome, info["tipo"], info["original"], info["filtrado"], info["removidas"]
    ))

