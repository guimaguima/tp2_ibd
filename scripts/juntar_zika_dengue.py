import os
import pandas as pd

#mudar localizacao aqui!
PASTA = "../data/"
ARQUIVOS = [f for f in os.listdir(PASTA) if f.endswith(".csv")]

COLUNAS_PERMITIDAS = [
    "id", "dt_notific", "sorotipo", "ano_nasc", "dt_invest", "sexo", "resul_soro", "vomito", "gestante",
    "sangram", "dt_sin_pri", "classi_fin", "criterio", "tpautocto", "resul_vi_n", "doenca_tra", "evolucao", 
    "dt_obito", "dt_encerra", "dt_digita", "nduplic_n", "febre", "acido_pept", "alrm_abdom", "alrm_hemat", 
    "alrm_hipot", "alrm_letar", "alrm_liq", "alrm_plaq", "alrm_sang", "alrm_vom", "artralgia", "artrite", 
    "auto_imune", "cefaleia", "clinic_chik", "complica", "con_fhd", "conjuntvit", "diabetes", "dor_costas", 
    "dor_retro", "dt_alrm", "dt_chik_s1", "dt_chik_s2", "dt_grav", "dt_ns1", "dt_pcr", "dt_prnt", "dt_soro", 
    "dt_viral", "epistaxe", "evidencia", "exantema", "gengivo", "grav_ast", "grav_consc", "grav_conv", 
    "grav_ench", "grav_extre", "grav_hemat", "grav_hipot", "grav_insuf", "grav_melen", "grav_metro", 
    "grav_mioc", "grav_orgao", "grav_pulso", "grav_sang", "grav_taqui", "hematolog", "hematura", 
    "hepatopat", "hipertensa", "histopa_n", "imunoh_n", "laco", "laco_n", "leucopenia", "mani_hemor", 
    "metro", "mialgia", "resul_prnt", "nausea", "petequia_n", "petequias", "plaq_menor", "plasmatico", 
    "renal", "res_chiks1", "res_chiks2", "resul_ns1", "resul_pcr_", "cod_mun_infec", "cod_mun_res", 
    "ubs_not", "tipo_not", "tipo_infec", "raca", "escolaridade"
]

RENOMEAR_COLUNAS = {
    "cs_escol_n": "escolaridade",
    "cs_gestant": "gestante",
    "cs_sexo": "sexo",
    "cs_raca": "raca",
    "clinc_chik": "clinic_chik",
    "comuninf": "cod_mun_infec",
    "id_mn_resi": "cod_mun_res",
    "id_unidade": "ubs_not",
    "tp_not": "tipo_not"
}

colunas_encontradas = set()

for nome_arquivo in ARQUIVOS:
    caminho = os.path.join(PASTA, nome_arquivo)
    if nome_arquivo.startswith(("DENGBR", "ZIKABR")):
        cols = pd.read_csv(caminho, nrows=0).columns
        cols = [col.lower() for col in cols]
        cols = [RENOMEAR_COLUNAS.get(col, col) for col in cols]
        colunas_encontradas.update(cols)

colunas_usadas = [col for col in COLUNAS_PERMITIDAS if col in colunas_encontradas or col in ["id", "tipo_infec"]]

faltantes = [col for col in COLUNAS_PERMITIDAS if col not in colunas_usadas]
if faltantes:
    print(f"Colunas nunca presentes: {faltantes}")

saida_final = os.path.join(PASTA, "dados_unificados.csv")
if os.path.exists(saida_final):
    os.remove(saida_final)

id_global = 1
auditoria = []

for nome_arquivo in ARQUIVOS:
    caminho = os.path.join(PASTA, nome_arquivo)

    if nome_arquivo.startswith("DENGBR"):
        tipo = "dengue"
    elif nome_arquivo.startswith("ZIKABR"):
        tipo = "zika"
    else:
        continue

    total_lidas = 0
    total_mantidas = 0
    id_inicial_arquivo = id_global

    for i, chunk in enumerate(pd.read_csv(caminho, dtype=str, low_memory=False, chunksize=5000)):
        total_lidas += len(chunk)
        chunk.columns = [col.lower() for col in chunk.columns]
        chunk.rename(columns=RENOMEAR_COLUNAS, inplace=True)

        for col in colunas_usadas:
            if col not in chunk.columns:
                chunk[col] = pd.NA

        chunk = chunk[colunas_usadas].copy()
        chunk["tipo_infec"] = tipo
        chunk["id"] = range(id_global, id_global + len(chunk))
        id_global += len(chunk)

        total_mantidas += len(chunk)

        chunk.to_csv(saida_final, mode='a', index=False, header=(i==0 and nome_arquivo==ARQUIVOS[0]), encoding="utf-8")

    id_final_arquivo = id_global - 1
    auditoria.append({
        "arquivo": nome_arquivo,
        "tipo": tipo,
        "linhas_lidas": total_lidas,
        "linhas_mantidas": total_mantidas,
        "id_inicial": id_inicial_arquivo,
        "id_final": id_final_arquivo
    })

print(f"\nArquivo final salvo em {saida_final}\n")

print("AUDITORIA:")
for entry in auditoria:
    print(f"\nArquivo: {entry['arquivo']}")
    print(f"Tipo de infecção: {entry['tipo']}")
    print(f"Linhas lidas: {entry['linhas_lidas']}")
    print(f"Linhas mantidas: {entry['linhas_mantidas']}")
    print(f"IDs atribuídos: {entry['id_inicial']} a {entry['id_final']}")

