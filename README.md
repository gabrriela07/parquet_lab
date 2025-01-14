# 🔬 PARQUET LAB
#### Conversor de CSV para Parquet e Armazenamento no DuckDB
---
Este projeto foi desenvolvido em Python e realiza o processamento de arquivos CSV contidos em arquivos ZIP, convertendo-os para o formato Parquet e armazenando informações em um banco de dados DuckDB. 

## Funcionalidades
- **Extração de Arquivos ZIP**: Percorre uma pasta de arquivos ZIP e extrai os arquivos CSV contidos nela.
- **Conversão para Parquet**: Realiza a conversão dos arquivos CSV para Parquet, incluindo:
  - Criação de um novo diretório para armazenar os arquivos convertidos.
  - Exclusão dos arquivos CSV antigos após a conversão.
- **Cálculo de Economia de Espaço**: Calcula a redução no tamanho dos arquivos após a conversão.
- **Armazenamento no DuckDB**: Registra as informações de economia de espaço no banco de dados DuckDB.
- **Monitoramento em Tempo Real**: Exibe prints no console com o progresso e tempo de execução dos processos.

## Tecnologias Utilizadas
O projeto foi desenvolvido em Python e utiliza as seguintes bibliotecas e ferramentas:
- **Python Padrão**:
  - `os` para manipulação de diretórios e arquivos.
  - `zipfile` para extração de arquivos ZIP.
  - `glob` para busca de arquivos.
  - `datetime` e `time` para manipulação de datas e medições de tempo.
- **Bibliotecas Externas**:
  - `pandas` para manipulação de dados tabulares.
  - `duckdb` para armazenamento no banco de dados.
- **Módulos Customizados**:
  - `src.utils` para funções utilitárias como:
    - `handle_count_files`
    - `handle_size_files`
    - `handle_unzip_files`
    - `handle_new_dir_parquet`
    - `handle_csv_to_parquet`
    - `handle_clean_tmp`
  - `src.models` para operações em camadas específicas:
    - `marts` (opções de marts)
    - `sources` (opções de fontes)
    - `staging` (query, update e criação).

## Configuração do Ambiente
1. **Requisitos**:
   - Python 3.8 ou superior.
   - Instale as dependências listadas no arquivo `requirements.txt`.

2. **Configuração do Banco de Dados**:
   Certifique-se de definir o caminho correto para o banco de dados DuckDB no módulo `utils`:
   ```python
   con = duckdb.connect(utils.DUCKDB_DATABASE)


## Contato

Se tiver dúvidas ou sugestões, entre em contato pelo e-mail: [gabriela.grc@outlook.com](mailto:gabriela.grc@outlook.com).

Feito com ❤ por Gabriela G👻
