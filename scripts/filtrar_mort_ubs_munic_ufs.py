import os
import pandas as pd

PASTA = "dados"
ARQUIVOS = [f for f in os.listdir(PASTA) if f.endswith(".csv")]
CHUNKSIZE = 10000

log_remocoes = {}

def normaliza_colunas(cols):
    return [str(c).strip().lower() for c in cols]

def filtro_mortalidade(df):
    obrigatorias = [
        "origem","tipobito","dtobito","horaobito","lococor","codmunocor","causabas","dtatestado","numerolote","causabas_o","dtcadastro","dtrecebim","atestado","dtrecoriga"
    ]
    obrigatorias = [c.lower() for c in obrigatorias]  
    mask = df[obrigatorias].notna().all(axis=1)
    return mask

def filtro_municipios(df):
    obrigatorias = [
        "codigo_ibge","codigo_uf","nome" 
    ]
    obrigatorias = [c.lower() for c in obrigatorias]
    mask = df[obrigatorias].notna().all(axis=1)
    return mask

def filtro_ufs(df):
    obrigatorias = [
        "codigo","sigla","nome"
    ]
    obrigatorias = [c.lower() for c in obrigatorias]
    mask = df[obrigatorias].notna().all(axis=1)
    return mask


for nome_arquivo in ARQUIVOS:
    caminho = os.path.join(PASTA, nome_arquivo)

    
    if nome_arquivo.startswith("Mortalidade"):
        tipo = "mortalidade"
        filtro = filtro_mortalidade
    elif nome_arquivo.startswith("municipios"):
        tipo = "municipios"
        filtro = filtro_municipios
    elif nome_arquivo.startswith("ufs"):
        tipo = "ufs"
        filtro = filtro_ufs
    else:
        print(f"Ignorando {nome_arquivo} (prefixo desconhecido)")
        continue

    print(f"🔍 Processando {nome_arquivo} ({tipo})")

    try:
        colunas_originais_raw = pd.read_csv(caminho, nrows=0, sep=";").columns.tolist()
        colunas_originais = normaliza_colunas(colunas_originais_raw)
    except Exception as e:
        print(f"Erro ao ler cabeçalho de {nome_arquivo}: {e}")
        continue

    original_total = 0
    filtrado_total = 0
    saida = os.path.join(PASTA, nome_arquivo.replace(".csv", "_filtrado.csv"))
    header_escrito = False

    try:
        for i, chunk in enumerate(pd.read_csv(caminho, chunksize=CHUNKSIZE, dtype=str, low_memory=False, sep=";")):
            chunk.columns = normaliza_colunas(chunk.columns)

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

print("\n RESUMO DA LIMPEZA:")
print("{:<25} {:<8} {:<10} {:<10} {:<10}".format("Arquivo", "Tipo", "Original", "Mantidas", "Removidas"))
for nome, info in log_remocoes.items():
    print("{:<25} {:<8} {:<10} {:<10} {:<10}".format(
        nome, info["tipo"], info["original"], info["filtrado"], info["removidas"]
    ))
