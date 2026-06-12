# INSTRUÇÕES.md — guia para o novo desenvolvedor

Bem-vindo ao **climasus4py**! Este guia foi escrito para quem está começando: ele explica o projeto, como preparar o ambiente, como usar Git no dia a dia, como documentar e como trabalhar com a IA. Leia uma vez do início ao fim; depois, use como consulta.

> **Regra número zero:** em dúvida, pergunte ao coordenador (Marlon — `marlonresendefaria@gmail.com`). Perguntar cedo é sempre mais barato do que desfazer depois.

---

## 1. O que é este projeto

O `climasus4py` é um pacote Python para integrar dados de saúde pública do **SUS/DATASUS** com dados **climáticos e socioeconômicos** do Brasil. Ele é o "irmão Python" do pacote R `climasus4r`: as duas bibliotecas têm **as mesmas funções, com os mesmos nomes e os mesmos argumentos** (chamamos isso de *paridade*). Por baixo, usa **DuckDB** (um banco de dados analítico embutido) para processar dados grandes sem estourar a memória.

Três consequências práticas disso para você:

1. **Não renomeie nem mude assinatura de funções públicas** (as que começam com `sus_`). Elas estão travadas em paridade com o pacote R.
2. **Não escreva "dados de domínio" no código** (códigos de doenças CID, nomes de estados, dicionários DATASUS). Tudo isso vem do pacote `climasus-data`, que é a fonte da verdade.
3. **As funções internas trabalham em modo "lazy"** (preguiçoso): elas montam a consulta no DuckDB mas só executam no final. Evite converter para pandas (`.df()`) no meio do caminho.

Esses princípios estão detalhados no [`CLAUDE.md`](CLAUDE.md) — o arquivo que instrui a IA. Vale a pena ler também.

---

## 2. Preparando o ambiente (uma vez só)

Você vai precisar de:

- **Python 3.10 ou superior** — [python.org/downloads](https://www.python.org/downloads/)
- **Git** — [git-scm.com](https://git-scm.com/)
- **uv** (gerenciador de pacotes do projeto) — [docs.astral.sh/uv](https://docs.astral.sh/uv/getting-started/installation/)
- **VS Code** (recomendado) — com as extensões *Python* e *Ruff*

Depois de instalar, no terminal, dentro da pasta do projeto:

```bash
# instala todas as dependências (incluindo ferramentas de desenvolvimento)
uv sync --extra dev

# confirma que tudo funciona rodando os testes
uv run pytest
```

Se os testes passarem, seu ambiente está pronto. Se algo falhar já na instalação, não gaste horas sozinho — pergunte.

---

## 3. Git no dia a dia

### Conceitos em uma linha cada

- **Commit** — uma "foto" salva das suas mudanças, com uma mensagem explicando o quê e por quê.
- **Branch** — uma linha de trabalho paralela. A `main` é a oficial; você trabalha em branches separadas.
- **Push** — enviar seus commits para o GitHub.
- **Pull Request (PR)** — pedido para incorporar sua branch na `main`, onde o código é revisado antes.

### O fluxo de toda tarefa

```bash
# 1. Atualize sua main local antes de começar
git switch main
git pull

# 2. Crie uma branch para a tarefa (nome curto e descritivo)
git switch -c fix/encoding-municipios

# 3. Trabalhe. A cada passo pequeno e completo, faça um commit:
git status                 # veja o que mudou ANTES de adicionar
git add caminho/arquivo.py # adicione arquivo por arquivo (evite "git add .")
git commit -m "fix(clean): normalize municipality encoding"

# 4. Envie a branch para o GitHub
git push -u origin fix/encoding-municipios

# 5. Abra um Pull Request no GitHub e peça revisão
```

### Mensagens de commit

Usamos o padrão **Conventional Commits**: `tipo(escopo): resumo curto`. Exemplos reais deste repositório:

```
fix(inmet): clean station filter materialization
perf(inmet): compact final dataframe collection
chore(release): bump climasus4py to 0.2.0a4
docs: add usage example for sus_filter
test(filter): cover sex synonym expansion
```

Tipos mais comuns: `feat` (funcionalidade nova), `fix` (correção), `perf` (desempenho), `docs`, `test`, `chore` (manutenção).

### Regras de ouro (memorize estas)

1. **Nunca trabalhe direto na `main`.** Sempre crie uma branch.
2. **Commits pequenos e frequentes.** Um commit = uma mudança lógica. Vários commits pequenos são melhores que um gigante.
3. **`git status` antes de todo commit.** Confira o que está entrando.
4. **Nunca use `git push --force`**, nem `git reset --hard` em algo já enviado. Se o Git "travar" e sugerir força, pare e pergunte.
5. **Nunca commite segredos:** arquivos `.env`, tokens, senhas, chaves. O `.gitignore` já protege os casos comuns — não o contorne.
6. Se você se perder ("o Git está estranho"), **não tente comandos aleatórios da internet**. Anote o que fez, e pergunte (ao Marlon ou à IA, em modo análise).

---

## 4. Qualidade: rode antes de cada commit

```bash
uv run pytest              # testes — tudo precisa passar
uv run ruff check .        # lint — estilo e erros comuns
uv run mypy climasus4py    # checagem de tipos
```

Mudou ou criou uma função? **Escreva ou atualize o teste correspondente** em `tests/`. Uma correção de bug sempre ganha um teste que falhava antes e passa depois.

---

## 5. Documentação

Há três camadas, com idiomas diferentes — essa separação é proposital:

| Onde | Idioma | O que é |
| --- | --- | --- |
| Docstrings (no código) | **Inglês** | Documentação de cada função, estilo Google (`Args/Returns/Raises`) |
| `CHANGELOG.md` | **Português** | Registro de toda mudança visível ao usuário, por versão |
| `docs/` (MkDocs) | Português (com versões EN/ES quando aplicável) | Guias e tutoriais do site |

### Docstring — exemplo do padrão do projeto

```python
def expand_sex_synonyms(sex: str | list[str]) -> list[str]:
    """Expand sex label(s) to the canonical DATASUS codes (``"1"`` / ``"2"``).

    Args:
        sex: A sex value or list of sex values. Accepts DATASUS codes,
            canonical letters (``"M"``, ``"F"``), and full names.

    Returns:
        Deduplicated list of DATASUS sex codes.

    Raises:
        ValueError: If any value is not recognised.
    """
```

### CHANGELOG

Toda mudança que o usuário do pacote percebe (função nova, bug corrigido, comportamento alterado) ganha uma entrada no `CHANGELOG.md`, na seção da versão em desenvolvimento, seguindo o formato que já está no arquivo. Escreva em português, citando a função e o arquivo afetado.

### Site de documentação

Para visualizar o site localmente:

```bash
uv run --with-requirements requirements-docs.txt mkdocs serve
```

---

## 6. Trabalhando com a IA

O arquivo [`CLAUDE.md`](CLAUDE.md) na raiz instrui a IA automaticamente sobre a filosofia do projeto. **Não edite nem apague esse arquivo** sem falar com o Marlon.

Boas práticas que vão te poupar muita dor:

1. **Peça análise antes de implementação.** Primeiro: "analise o problema X e me proponha opções com prós e contras". Só depois: "implemente a opção 2".
2. **Revise todo diff antes de aceitar.** Você é responsável pelo que entra no repositório, não a IA. Se não entendeu uma mudança, peça: *"explique linha por linha o que você mudou e por quê, como se eu fosse iniciante"*.
3. **Para mudanças grandes, peça um plano por escrito primeiro** e envie ao Marlon antes de executar.
4. **Desconfie de nomes inventados.** Se a IA citar uma coluna, código CID ou função que você não consegue encontrar no código ou no `climasus-data`, questione — esse é o erro mais comum.
5. **Use a IA como professora.** "Por que esta função retorna DuckDBPyRelation em vez de DataFrame?" é uma ótima pergunta — a resposta te ensina a filosofia do projeto.

---

## 7. O que NUNCA fazer sem falar com o Marlon antes

- Renomear, remover ou mudar argumentos de qualquer função pública (`sus_*`).
- Alterar arquivos JSON do pacote `climasus-data`.
- Publicar no PyPI, criar tag ou release.
- Adicionar dependência nova ao `pyproject.toml`.
- "Corrigir" um comportamento que veio do pacote R — a diretriz é replicar o R e **anotar** a observação (ver §8). Exceção: falhas de segurança e resultados silenciosamente errados são corrigidos, com registro no `CHANGELOG.md`.
- Qualquer comando Git com `--force`.

---

## 8. IDEIAS.md — onde anotar em vez de executar

Durante o trabalho você (ou a IA) vai notar coisas melhoráveis: um bug herdado do R, um nome confuso, um código lento. **Não conserte por conta própria** — registre em [`IDEIAS.md`](IDEIAS.md) com uma linha de contexto. Esse backlog alimenta a versão 2.0 e é revisado periodicamente com o coordenador.

---

## 9. Glossário rápido

- **Lazy (avaliação preguiçosa)** — a consulta é montada mas só executa quando o resultado é pedido. Permite processar dados maiores que a memória.
- **DuckDB** — banco de dados analítico embutido (roda dentro do Python, sem servidor). É o motor do pacote.
- **`DuckDBPyRelation`** — o objeto "consulta lazy" do DuckDB que as funções internas passam entre si.
- **Arrow / Parquet** — formatos colunares eficientes para troca e armazenamento de dados.
- **Paridade R↔Python** — garantia de que `climasus4r` e `climasus4py` têm a mesma API pública.
- **CID-10** — classificação internacional de doenças, usada nos dados do DATASUS.
- **Wheel** — pacote Python pré-compilado; o usuário final instala sem precisar de compilador.
- **Lint** — verificação automática de estilo e erros comuns no código (aqui, `ruff`).
- **PR (Pull Request)** — pedido de revisão e integração de uma branch na `main`.

---

## 10. Links e contatos

- **Coordenador:** Marlon (MarlonRF) — `marlonresendefaria@gmail.com`
- **Organização no GitHub:** [github.com/climasus](https://github.com/climasus)
- **Portal do projeto:** [climasus.github.io](https://climasus.github.io)
- **Pacote R de referência (paridade):** [github.com/ByMaxAnjos/climasus4r](https://github.com/ByMaxAnjos/climasus4r)
- **Instruções da IA:** [`CLAUDE.md`](CLAUDE.md) · **Backlog:** [`IDEIAS.md`](IDEIAS.md) · **Histórico:** [`CHANGELOG.md`](CHANGELOG.md)
