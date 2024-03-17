import argparse
import logging
import apache_beam as beam
import os
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.dataframe.convert import to_pcollection
from datetime import datetime


pipeline_options = {
    'project': 'facens-411114' ,
    'runner': 'DataflowRunner',
    'region': 'us-central1',
    'staging_location': 'gs://sinasc_bronze/temp',
    'temp_location': 'gs://sinasc_bronze/temp',
    'template_location': 'gs://sinasc_template/batch_sinasc_data_flow', 
    'requirements_file':'requirements.txt',
    'save_main_session' : True 
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
pipeline = beam.Pipeline(options=pipeline_options)

def transform_to_dict_escolaridade(data):
    return {
        'id_escmae': data[0],
        'escolaridade': data[1],
    }

def transform_to_dict_profissao(data):
    return {
        'id_ocupmae': int(data[0]),
        'ocupacao': data[1],
    }

def transform_to_dict_gestacao(data):
    return {
        'id_gestacao': data[0],
        'descricao_semanas': data[1],
    }

def transform_to_dict_municipio(data):
    return {
        'id_codmunicipionasc': int(data[0]),
        'cidade': data[1],
        'estado': data[2]
    }

def transform_to_dict_mae(data):
    return {
        'id_mae': data[0],
        'id_escmae': data[1],
        'id_codocupmae':data[2],
        'dtnascmae': data[3],
        'id_gestacao': data[4],
        'raca_mae': data[5],
        'estado_civil': data[6],
        'qtd_filhovivo': data[7],
        'qtd_filhomorto': data[8],
    }

def transform_to_dict_parto_sexo(data):
    return {
        'id_parto_sexo': data[0],
        'sexo': data[2],
        'parto': data[1]
    }

def transform_to_dict_consultas(data):
    return {
        'id_consultas': data[0],
        'descricao': data[1],
    }

def transform_to_dict_nascidos(data):
    return {
        'id_mae': data[0],
        'id_calendario': data[1],
        'id_codmunnasc': data[2],
        'id_parto_sexo': data[3],
        'id_consulta': data[4],
        'raca_nascidos': data[5],
        'local_nascimento': data[6],
    }

def importacao_de_dados_sinasc(file):
    columns = [
        "ESCMAEAGR1",
        "CONSULTAS",
        "SEXO",
        "PARTO",
        "GESTACAO",
        "CONTADOR",
        "ESCMAE",
        "CODOCUPMAE",
        "DTNASCMAE",
        "GESTACAO",
        "RACACORMAE",
        "ESTCIVMAE",
        "QTDFILVIVO",
        "QTDFILMORT",
        "DTNASC",
        "CODMUNNASC",
        "RACACOR",
        "LOCNASC",
    ]
    df = pd.read_csv(file, encoding='ISO-8859-1',quotechar='"',usecols=columns, delimiter=';')
    df = df.dropna()
    df['DTNASC'] = df.apply(lambda row:  datetime.strptime('0' + str(int(row['DTNASC'])), "%d%m%Y")  if len(str(int(row['DTNASC']))) < 8 else datetime.strptime(str(int(row['DTNASC'])), "%d%m%Y") , axis=1)
    df['DTNASCMAE'] = df.apply(lambda row:  datetime.strptime('0' + str(int(row['DTNASCMAE'])), "%d%m%Y")  if len(str(int(row['DTNASCMAE']))) < 8 else datetime.strptime(str(int(row['DTNASCMAE'])), "%d%m%Y") , axis=1)
    df['DTNASCMAE'] = pd.to_datetime(df['DTNASCMAE'], format='%Y-%m-%d', errors='coerce')
    df = df.loc[(df['DTNASCMAE'] >= '1950-01-01')
                     & (df['DTNASCMAE'] < '2014-01-01')]
    df = df[
        (df['QTDFILVIVO'] <= 10) & (df['QTDFILMORT'] <= 10)
    ]
    df = df.sample(n=500000)
    df['CODMUNNASC'] = df['CODMUNNASC'].apply(str)
    df['ESCMAE'] = df['ESCMAE'].apply(int)
    df['CONSULTAS'] = df['CONSULTAS'].apply(int) 
    df['SEXO'] = df['SEXO'].apply(str)
    df['PARTO'] = df['PARTO'].apply(str) 
    df['GESTACAO'] = df['GESTACAO'].apply(int) 
    df['CONTADOR'] = df['CONTADOR'].apply(int) 
    df['ESCMAE'] = df['ESCMAE'].apply(int) 
    df['CODOCUPMAE'] = df['CODOCUPMAE'].apply(int) 
    df['GESTACAO'] = df['GESTACAO'].apply(int)
    df['QTDFILVIVO'] = df['QTDFILVIVO'].apply(int)
    df['QTDFILMORT'] = df['QTDFILMORT'].apply(int)
    df['LOCNASC'] = df['LOCNASC'].apply(int)
    return df

def importacao_de_dados_municipio(file):
    df = pd.read_csv(file, delimiter=";", dtype={'Codigo_Municipio_Completo': object}) 
    df = df[['Codigo_Municipio_Completo','Nome_Municipio','Nome_UF']]
    df['Codigo_Municipio_Completo'] = df['Codigo_Municipio_Completo'].str.slice(stop=-1)
    df['UF'] = df['Nome_UF'].map(estados_dict)
    df = df[['Codigo_Municipio_Completo','Nome_Municipio','UF']]
    return df

def importacao_de_dados_ocupacao(file):
    df = pd.read_csv(file, delimiter=";", encoding = "ISO-8859-1" ) 
    return df


def adicionar_descricao_escolaridade(record):
    codigo_escolaridade = int(record)
    descricao_escolaridade = escolaridade_dict.get(codigo_escolaridade, None)
    elemento_com_descricao = (record, descricao_escolaridade)
    return elemento_com_descricao


def adicionar_descricao_consulta(record):
    consulta = int(record)
    descricao_consulta = consultas_dict.get(consulta, None)
    elemento_com_descricao = (record, descricao_consulta)
    return elemento_com_descricao

def adicionar_parto_sexo(record):
    values = record.split(',')
    parto = values[1] 
    sexo = values[0]  
    descricao_sexo = sexo_dict.get(int(float(sexo)), None)
    descricao_parto = parto_dict.get(int(float(parto)), None)
    id_parto_sexo = str(int(float(parto))) + str(int(float(sexo)))
    elemento_com_descricao = (int(id_parto_sexo), descricao_parto,descricao_sexo )
    return elemento_com_descricao


def adicionar_gestacao(record):
    gestacao = int(record)
    descricao_gestacao = gestacao_dict.get(gestacao, None)
    elemento_com_descricao = (gestacao, descricao_gestacao)
    return elemento_com_descricao

def adicionar_colunas_raca_estado_civil(record):
     values = record.split(',')
     raca = int(float(values[5]))
     descricao_raca = raca_dict.get(raca, None)
     estado_civil = int(float(values[6]))
     descricao_estado_civil = estado_civil_dict.get(estado_civil, None)
     data: str = values[3]
     data_format = data.rstrip(' 00:00:00')
     elemento_com_descricao = int(values[0]),int(values[1]),int(values[2]),data_format,int(values[4]),descricao_raca,descricao_estado_civil,int(values[7]),int(values[8])
     return elemento_com_descricao

def adicionar_colunas_fato(record):
     values = record.split(',')
     raca = int(float(values[5]))
     descricao_raca = raca_dict.get(raca, None)
     local_nascimento = int(float(values[6]))
     descricao_local_nascimento = local_nascimento_dict.get(local_nascimento, None)
     id_parto_sexo = str(int(float(values[3]))) + str(int(float(values[4])))
     data: str = values[1]
     data_format = data.rstrip(' 00:00:00')
     elemento_com_descricao = int(values[0]),data_format,int(values[2]),int(id_parto_sexo),int(values[5]),descricao_raca,descricao_local_nascimento
     return elemento_com_descricao



escolaridade_dict = {
    0  : 'Sem Escolaridade',
    1  : 'Fundamental I Incompleto',
    2  : 'Fundamental I Completo',
    3  : 'Fundamental II Incompleto',
    4  : 'Fundamental II Completo',
    5  : 'Ensino Médio Incompleto',
    6  : 'Ensino Médio Completo',
    7  : 'Superior Incompleto',
    8  : 'Superior Completo',
    9  : 'Ignorado',
    10 : 'Fundamental I Incompleto ou Inespecífico',
    11 : 'Fundamental II Incompleto ou Inespecífico',
    12 : 'Ensino Médio Incompleto ou Inespecífico'
}

consultas_dict = {
     1 : 'Nenhuma',
     2 : 'de 1 a 3',
     3 : 'de 4 a 6',
     4 : '7 e mais',
     9 : 'Ignorado'
}

sexo_dict = {
    1 : 'M – Masculino',
    2 : 'F – Feminino',
    0 : 'I – Ignorado'
}

parto_dict = {
    1 : 'Vaginal',
    2 : 'Cesário',
    9 : 'Ignorado'
}

gestacao_dict = {
    1 : 'Menos de 22 semanas', 
    2 : '22 a 27 semanas',
    3 : '28 a 31 semanas', 
    4 : '32 a 36 semanas', 
    5 : '37 a 41 semanas',
    6 : '42 semanas e mais', 
    9 : 'Ignorado'
}

gravidez_dict = {
     '1' : 'Única', 
     '2' : 'Dupla', 
     '3' : 'Tripla ou mais', 
     '9' : 'Ignorado'   
}

raca_dict = {
     1 : 'Branca', 
     2 : 'Preta', 
     3 : 'Amarela', 
     4 : 'Parda',
     5 : 'Indígena'   
}

estado_civil_dict = {
     1 : 'Solteira', 
     2 : 'Casada', 
     3 : 'Viúva', 
     4 : 'Separada judicialmente/divorciada', 
     5 : 'União estável', 
     9 : 'Ignorada'
}

estados_dict = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO"
}

local_nascimento_dict = {
    1 : "Hospital",
    2 : "Outros estabelecimentos de saúde",
    3 : "Domicílio",
    4 : "Outros",
    5 : "Aldeia Indígena"
}

serviceAccount = r'./facens-411114-6dcdac6bcd20.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= serviceAccount

bucket_name = 'sinasc_bronze'

df_municipio = importacao_de_dados_municipio('gs://sinasc_bronze/source/RELATORIO_DTB_BRASIL_MUNICIPIO.csv')

df_ocupacao = importacao_de_dados_ocupacao('gs://sinasc_bronze/source/CBO2002_Ocupacao.csv')

df_sinasc = importacao_de_dados_sinasc('gs://sinasc_bronze/source/DNOPEN22.csv')


resultado_municipio = pd.merge(df_sinasc, df_municipio, left_on='CODMUNNASC', right_on='Codigo_Municipio_Completo', how='inner')
resultado_municipio = resultado_municipio[['CODMUNNASC','Nome_Municipio','UF']]

resultado_ocupacao = pd.merge(df_sinasc, df_ocupacao, left_on='CODOCUPMAE', right_on='CODIGO', how='inner')
resultado_ocupacao = resultado_ocupacao[['CODOCUPMAE','TITULO']]
resultado_ocupacao['CODOCUPMAE'] = resultado_ocupacao['CODOCUPMAE'].astype(int)

tabela_dim_escolaridade = 'facens-411114.dw.dim_escolaridade'
tabela_dim_profissao = 'facens-411114.dw.dim_profissao'
tabela_dim_gestacao = 'facens-411114.dw.dim_gestacao'
tabela_dim_mae = 'facens-411114.dw.dim_mae'
tabela_dim_municipio = 'facens-411114.dw.dim_municipio'
tabela_dim_parto_sexo = 'facens-411114.dw.dim_parto_sexo'
tabela_dim_consultas = 'facens-411114.dw.dim_consultas'
tabela_fatos_nascidos = 'facens-411114.dw.fatos_nascidos'


logging.getLogger().setLevel(logging.INFO)
_, options = argparse.ArgumentParser().parse_known_args()

sinasc = (
  to_pcollection(df_sinasc, pipeline=pipeline)
)

dim_municipio = (
    to_pcollection(resultado_municipio, pipeline=pipeline)
    | "Remove linhas duplicadas da dimensão municipio" >> beam.Distinct()
    | "Transformando em dicionário municipio" >> beam.Map(lambda line: transform_to_dict_municipio(line))
    | "Escrevendo no BigQuery dim_municipio" >> beam.io.WriteToBigQuery(
     tabela_dim_municipio,
     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
     custom_gcs_temp_location = 'gs://sinasc_bronze/tmp')    
)

dim_profissao = (
    to_pcollection(resultado_ocupacao, pipeline=pipeline)
    | "Remove linhas duplicadas da dimensão profissao" >> beam.Distinct()
    | "Transformando em Dicionário Profissão" >> beam.Map(lambda line: transform_to_dict_profissao(line))
    | 'Escrevendo no BigQuery dim_profissao' >> beam.io.WriteToBigQuery(
    tabela_dim_profissao,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    custom_gcs_temp_location = 'gs://sinasc_bronze/tmp')
)

dim_escolaridade = (
   sinasc
  | "Criar dataset com dados de escolaridade" >> beam.Map(lambda x: x[3])
  | "Remove duplicatas da escolaridade" >> beam.Distinct()
  | "Adicionar coluna descrição escolaridade" >> beam.Map(lambda line:adicionar_descricao_escolaridade(line))
  | "Transformando em Dicionário escolaridade" >> beam.Map(lambda line: transform_to_dict_escolaridade(line))
  | "Escrevendo no BigQuery dim_escolaridade" >> beam.io.WriteToBigQuery(
    tabela_dim_escolaridade,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    custom_gcs_temp_location = 'gs://sinasc_bronze/tmp')
)

dim_consultas = (
   sinasc
  | "Criar dataset com dados de consultas" >> beam.Map(lambda x: x[9])
  | "Remove duplicatas de consultas" >> beam.Distinct()
  | "Adicionar coluna descrição consulta" >> beam.Map(lambda line:adicionar_descricao_consulta(line))
  | "Transformando em Dicionário dim_consultas" >> beam.Map(lambda line: transform_to_dict_consultas(line))
  | "Escrevendo no BigQuery dim_consultas" >> beam.io.WriteToBigQuery(
    tabela_dim_consultas,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    custom_gcs_temp_location = 'gs://sinasc_bronze/tmp')  
)

dim_parto_sexo = (
   sinasc
  | "Criar dataset com dados de sexo e parto" >> beam.Map(lambda x: x[11] + ',' + x[8]  )
  | "Remove duplicatas sexo e parto" >> beam.Distinct()
  | "Adicionar colunas na tabela parto_sexo" >> beam.Map(lambda line: adicionar_parto_sexo(line))
  | 'Transformando em Dicionário parto_sexo' >> beam.Map(lambda line: transform_to_dict_parto_sexo(line))
  | 'Escrevendo no BigQuery parto e sexo' >> beam.io.WriteToBigQuery(
    tabela_dim_parto_sexo,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    custom_gcs_temp_location = 'gs://sinasc_bronze/tmp')  
)

dim_gestacao = (
    sinasc
    | "Criar dataset com dados de gestação" >> beam.Map(lambda x : x[7])
    | "Remover duplicatas gestação" >> beam.Distinct()
    | "Adicionar colunas na tabela gestacao" >> beam.Map(lambda line: adicionar_gestacao(line))
    | "Transformando em Dicionário gestacao" >> beam.Map(lambda line: transform_to_dict_gestacao(line))
    | "Escrevendo no BigQuery dim_gestacao" >> beam.io.WriteToBigQuery(
    tabela_dim_gestacao,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    custom_gcs_temp_location = 'gs://sinasc_bronze/tmp')
)

dim_mae = (
    sinasc
    | "Criar dataset com dados de mãe" >> beam.Map(lambda x : str(x[16]) + "," + str(x[3]) + "," + str(x[4]) +  "," + str(x[13]) + "," + str(x[7]) + "," + str(x[14]) + "," + str(x[2]) + "," + str(x[5]) + "," + str(x[6]))
    | "Remover duplicatas mãe" >> beam.Distinct()
    | "Adicionar colunas raca mãe e estado civil" >> beam.Map(lambda line: adicionar_colunas_raca_estado_civil(line))
    | "Transformando em Dicionário mãe" >> beam.Map(lambda line: transform_to_dict_mae(line))
    | "Escrevendo no BigQuery dim_mãe" >> beam.io.WriteToBigQuery(
     tabela_dim_mae,
     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
     custom_gcs_temp_location = 'gs://sinasc_bronze/tmp')
)

fato_nascidos = (
    sinasc 
    | "Criar dataset com dados da fato" >> beam.Map(lambda x : str(x[16]) + "," + str(x[10]) + "," + str(x[0]) + "," + str(x[8]) +  "," + str(x[11]) + "," +  str(x[9]) + "," + str(x[12]) + "," + str(x[1]))
    | "Remove duplicatas da fato" >> beam.Distinct()
    | "Enriquecimento dos dados" >> beam.Map(lambda line: adicionar_colunas_fato(line))
    | "Transformando em Dicionário Fato" >> beam.Map(lambda line: transform_to_dict_nascidos(line))
    | "Escrevendo no BigQuery fato nascidos" >> beam.io.WriteToBigQuery(
      tabela_fatos_nascidos,
      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
      custom_gcs_temp_location = 'gs://sinasc_bronze/tmp')
)

pipeline.run().wait_until_finish()
