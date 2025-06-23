-- Schema gerado automaticamente a partir do dicionario.csv

CREATE TABLE unidade_federativa (
  cod_uf NUMERIC(2) NOT NULL UNIQUE,
  sigla CHAR(2) NOT NULL UNIQUE,
  nome VARCHAR(22) NOT NULL UNIQUE,
  lat_uf REAL NOT NULL,
  long_uf REAL NOT NULL,
  regiao VARCHAR(12) NOT NULL,
  PRIMARY KEY (cod_uf)
);

CREATE TABLE municipios (
  cod_mun NUMERIC(7) NOT NULL UNIQUE,
  nome_mun VARCHAR(60) NOT NULL,
  fuso VARCHAR(30) NOT NULL,
  lat_mun REAL NOT NULL,
  long_mun REAL NOT NULL,
  ddd NUMERIC(2) NOT NULL,
  cod_uf NUMERIC(2) NOT NULL,
  PRIMARY KEY (cod_mun),
  FOREIGN KEY (cod_uf) REFERENCES unidade_federativa(cod_uf) ON DELETE RESTRICT
);

CREATE TABLE escolaridade (
  id INTEGER NOT NULL UNIQUE,
  "desc" VARCHAR(50) NOT NULL UNIQUE,
  PRIMARY KEY (id)
);

CREATE TABLE raca (
  id INTEGER NOT NULL UNIQUE,
  "desc" VARCHAR(12) NOT NULL UNIQUE,
  PRIMARY KEY (id)
);

CREATE TABLE tipo_notificacao (
  id INTEGER NOT NULL UNIQUE,
  tipo VARCHAR(10) NOT NULL UNIQUE,
  PRIMARY KEY (id)
);

CREATE TABLE tipo_infectado (
  id INTEGER NOT NULL UNIQUE,
  "desc" VARCHAR(6) NOT NULL UNIQUE,
  PRIMARY KEY (id)
);

CREATE TABLE notificacao_de_infectados (
  id INTEGER NOT NULL UNIQUE,
  dt_notific DATE,
  ano_nasc NUMERIC(4) NOT NULL,
  dt_invest DATE,
  cs_sexo CHAR(1) NOT NULL,
  vomito BOOLEAN,
  cs_gestant NUMERIC(1) NOT NULL,
  sangram BOOLEAN,
  dt_sin_pri DATE,
  classi_fin NUMERIC(3) NOT NULL,
  criterio NUMERIC(1),
  tpautocto NUMERIC(1),
  resul_vi_n NUMERIC(1),
  doenca_tra NUMERIC(1),
  evolucao NUMERIC(1),
  dt_obito DATE,
  dt_encerra DATE,
  dt_digita DATE,
  nduplic_n NUMERIC(1),
  sorotipo NUMERIC(1),
  resul_soro NUMERIC(1),
  febre BOOLEAN,
  acido_pept BOOLEAN,
  alrm_abdom BOOLEAN,
  alrm_hemat BOOLEAN,
  alrm_hipot BOOLEAN,
  alrm_letar BOOLEAN,
  alrm_liq BOOLEAN,
  alrm_plaq BOOLEAN,
  alrm_sang BOOLEAN,
  alrm_vom BOOLEAN,
  artralgia BOOLEAN,
  artrite BOOLEAN,
  auto_imune BOOLEAN,
  cefaleia BOOLEAN,
  clinic_chik NUMERIC(1),
  complica NUMERIC(1),
  con_fhd NUMERIC(1),
  conjuntvit BOOLEAN,
  diabetes BOOLEAN,
  dor_costas BOOLEAN,
  dor_retro BOOLEAN,
  dt_alrm DATE,
  dt_chik_s1 DATE,
  dt_chik_s2 DATE,
  dt_grav DATE,
  dt_ns1 DATE,
  dt_pcr DATE,
  dt_prnt DATE,
  dt_soro DATE,
  dt_viral DATE,
  epistaxe NUMERIC(1),
  evidencia NUMERIC(1),
  exantema NUMERIC(1),
  gengivo NUMERIC(1),
  grav_ast BOOLEAN,
  grav_consc BOOLEAN,
  grav_conv BOOLEAN,
  grav_ench BOOLEAN,
  grav_extre BOOLEAN,
  grav_hemat BOOLEAN,
  grav_hipot BOOLEAN,
  grav_insuf BOOLEAN,
  grav_melen BOOLEAN,
  grav_metro BOOLEAN,
  grav_mioc BOOLEAN,
  grav_orgao BOOLEAN,
  grav_pulso BOOLEAN,
  grav_sang BOOLEAN,
  grav_taqui BOOLEAN,
  hematolog BOOLEAN,
  hematura BOOLEAN,
  hepatopat BOOLEAN,
  hipertensa BOOLEAN,
  histopa_n NUMERIC(1),
  imunoh_n NUMERIC(1),
  laco BOOLEAN,
  laco_n NUMERIC(1),
  leucopenia BOOLEAN,
  mani_hemor NUMERIC(1),
  metro NUMERIC(1),
  mialgia BOOLEAN,
  resul_prnt NUMERIC(1),
  nausea BOOLEAN,
  petequia_n BOOLEAN,
  petequias NUMERIC(1),
  plaq_menor NUMERIC(1),
  plasmatico NUMERIC(1),
  renal NUMERIC(1),
  res_chiks1 NUMERIC(1),
  res_chiks2 NUMERIC(1),
  resul_ns1 NUMERIC(1),
  resul_pcr_ NUMERIC(1),
  cod_mun_infec NUMERIC(7) NOT NULL,
  cod_mun_res NUMERIC(7) NOT NULL,
  tipo_not INTEGER NOT NULL,
  tipo_infec INTEGER NOT NULL,
  raca INTEGER NOT NULL,
  escolaridade INTEGER NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (cod_mun_infec) REFERENCES municipios(cod_mun) ON DELETE RESTRICT,
  FOREIGN KEY (cod_mun_res) REFERENCES municipios(cod_mun) ON DELETE RESTRICT,
  FOREIGN KEY (tipo_not) REFERENCES tipo_infectado(id) ON DELETE RESTRICT,
  FOREIGN KEY (tipo_infec) REFERENCES tipo_notificacao(id) ON DELETE RESTRICT,
  FOREIGN KEY (raca) REFERENCES raca(id) ON DELETE RESTRICT,
  FOREIGN KEY (escolaridade) REFERENCES escolaridade(id) ON DELETE RESTRICT,
  CONSTRAINT chk_notificacao_de_infectados_cs_gestant_logic CHECK ((     (
        cs_sexo = 'F' AND
        (EXTRACT(YEAR FROM CURRENT_DATE) - ano_nasc) <= 10
        AND cs_gestant IN ('1', '6')
    )
    OR
    (
        cs_sexo = 'M' AND cs_gestant = '6'
    )
    OR
    (
        cs_sexo = 'I'
    )
    OR
  (
        cs_sexo = 'F' AND
        (EXTRACT(YEAR FROM CURRENT_DATE) - ano_nasc) > 10
        AND cs_gestant IN ('1', '2', '3', '4', '5', '6', '9')
    )))
);

