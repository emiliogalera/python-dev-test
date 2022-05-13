# Vamos começar?
Essa implementação necessita de um ambiente virtual. Não incluido na branch para não deixar o projeto pesado. Testado no bom e velho `linux mint`

K.I.S.S all the way! Vou manter as coisas simples.

    
1. Em um terminal inicie um ambiente virtual: `python3 -m venv env`

2. Inicie o ambiente virtual: `source env/bin/activate`

3. Atualize o pip: `pip install --upgrade pip`

4. Instale as bibliotecas através do arquivo **requirements.txt**: `pip install -r requirements.txt`

## Estrutura do projeto
- pasta data: Dados fornecidos pelos avaliadores, ver o README do projeto [aqui](README.md).

- env: Ambiente virtual criado pelos passos acima.

- tools: scripts criados para execução da tarefa. Coisas como funções de conexão de banco de dados, ferramententas de exploração, alteração, armazenamento de dados.

- teste.py: script para testar o funcionamento das ferramentas, fica junto com os scripts na pasta **tools**.

- `main.py`: script principal para execução do programa.

### Para rodar

Inicie o script `start.sh` através do comando:

> ./start.sh &

Gera um banco de dados para cada rodada. Fiz assim pois ao codificar colunas categóricas alguns valores aparecem em algumas rodadas e em outras não. Isso leva a uma inconsistência quando dados são inserido em rodadas diferentes.