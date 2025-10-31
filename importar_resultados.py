import pandas as pd
import re
import sys
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import datetime
import os

# --- IMPORTAÇÃO DE CLASSES E CONFIGURAÇÕES ---
# Importa as classes de modelo (Pesquisa, Processo, etc.) do app.py
try:
    from app import Cliente, Pesquisa, Processo, CapaProcesso, DocumentoInicial, Andamento, Parte, Advogado, \
        db as original_db
    from flask import Flask  # Necessário para criar o contexto de metadados

    app = Flask(__name__)
except ImportError:
    print("ERRO CRÍTICO: Falha ao importar classes do app.py. O app.py está na pasta raiz?")
    sys.exit(1)

# Importa a URL de conexão do arquivo de configuração local
try:
    from config_local import DATABASE_URL_REMOTE
except ImportError:
    print("ERRO CRÍTICO: O arquivo 'config_local.py' não foi encontrado. Crie e cole sua DATABASE_URL_REMOTE.")
    sys.exit(1)


# --- FUNÇÕES HELPER PARA CONEXÃO E DADOS ---

def get_remote_session():
    """ Cria uma sessão de banco de dados conectada ao PostgreSQL do Render. """
    print(f"Conectando ao banco de dados remoto...")

    # Ajusta o prefixo de conexão (PostgreSQL precisa do 'postgresql://')
    if DATABASE_URL_REMOTE.startswith("postgres://"):
        url = DATABASE_URL_REMOTE.replace("postgres://", "postgresql://", 1)
    else:
        url = DATABASE_URL_REMOTE

    engine = create_engine(url)

    # Garante que os modelos estejam mapeados para o motor (necessário para a primeira execução)
    with app.app_context():
        original_db.metadata.create_all(engine, checkfirst=True)

    Session = sessionmaker(bind=engine)
    return Session()


def extrair_oab(texto):
    """ Extrai o nome do advogado e o número OAB dos parênteses. """
    match = re.search(r'\((.*?)\)', texto)
    if match:
        oab = match.group(1)
        nome = texto.replace(f"({oab})", "").strip()
        return nome, oab
    return texto.strip(), None


# --- FUNÇÃO PRINCIPAL DE IMPORTAÇÃO ---

def importar_de_excel_v5():
    print("Iniciando importação de resultados (Remoto)...")

    try:
        df = pd.read_excel("resultados.xlsx")
    except FileNotFoundError:
        print("ERRO: Arquivo 'resultados.xlsx' não encontrado. Crie-o antes de rodar.")
        return
    except Exception as e:
        print(f"ERRO ao ler o Excel: {e}")
        return

    session = None
    try:
        session = get_remote_session()

        # Agrupa por 'codPesquisa' para processamento
        for cod_pesquisa, group in df.groupby('codPesquisa'):

            print(f"Processando codPesquisa: {cod_pesquisa}...")

            pesquisa_db = session.query(Pesquisa).get(int(cod_pesquisa))
            if not pesquisa_db:
                print(f"  Aviso: codPesquisa {cod_pesquisa} não encontrado no banco. Pulando.")
                continue

            # Dentro de cada pesquisa, agrupa por 'numeroProcesso'
            for num_processo, proc_group in group.groupby('numeroProcesso'):

                processo_db = session.query(Processo).filter(
                    Processo.pesquisa_id == pesquisa_db.id,
                    Processo.numero_processo == num_processo
                ).first()

                if not processo_db:
                    print(f"  Aviso: Processo {num_processo} não encontrado. Pulando.")
                    continue

                print(f"  Atualizando processo: {num_processo} (ID: {processo_db.id})")

                # Pega a *primeira* linha do grupo que contenha dados para Capa, Partes, etc.
                primeira_linha = proc_group.iloc[0]

                # --- 1. CAPA ---
                capa_existente = session.query(CapaProcesso).filter_by(processo_id=processo_db.id).first()
                if not capa_existente and pd.notna(primeira_linha.get('valorCausa')):
                    nova_capa = CapaProcesso(
                        processo_id=processo_db.id,
                        valor_causa=primeira_linha.get('valorCausa'),
                        classe_cnj=primeira_linha.get('classeCNJ'),
                        area=primeira_linha.get('area')
                    )
                    session.add(nova_capa)
                    print(f"    -> Capa criada.")

                # --- 2. PDF (URL S3) ---
                link_pdf = primeira_linha.get('pdfURL')
                if pd.notna(link_pdf):
                    doc_existente = session.query(DocumentoInicial).filter_by(processo_id=processo_db.id,
                                                                              link_documento=link_pdf).first()
                    if not doc_existente:
                        novo_doc = DocumentoInicial(
                            processo_id=processo_db.id,
                            link_documento=link_pdf,  # Salva o link S3 completo
                            documento_encontrado=True
                        )
                        session.add(novo_doc)
                        print(f"    -> Link S3 salvo.")

                # --- 3. PARTES ---
                partes_texto = primeira_linha.get('partes')
                if pd.notna(partes_texto):
                    partes_lista = str(partes_texto).split('|')
                    for p in partes_lista:
                        try:
                            tipo, nome = p.split(':', 1)
                            # Verifica se a parte já existe para evitar duplicatas
                            parte_existente = session.query(Parte).filter_by(processo_id=processo_db.id,
                                                                             tipo=tipo.strip(),
                                                                             nome=nome.strip()).first()
                            if not parte_existente:
                                session.add(Parte(processo_id=processo_db.id, tipo=tipo.strip(), nome=nome.strip()))
                                print(f"    -> Parte '{nome.strip()}' salva.")
                        except:
                            print(f"    -> ERRO: Formato inválido na coluna 'partes': {p}")

                # --- 4. ADVOGADOS ---
                advs_texto = primeira_linha.get('advogados')
                if pd.notna(advs_texto):
                    advs_lista = str(advs_texto).split('|')
                    for a in advs_lista:
                        try:
                            tipo, nome_oab = a.split(':', 1)
                            nome, oab = extrair_oab(nome_oab)
                            adv_existente = session.query(Advogado).filter_by(processo_id=processo_db.id,
                                                                              tipo=tipo.strip(), nome=nome).first()
                            if not adv_existente:
                                session.add(Advogado(processo_id=processo_db.id, tipo=tipo.strip(), nome=nome, oab=oab))
                                print(f"    -> Advogado '{nome}' salvo.")
                        except:
                            print(f"    -> ERRO: Formato inválido na coluna 'advogados': {a}")

                # --- 5. ANDAMENTOS (itera em TODAS as linhas) ---
                for _, row in proc_group.iterrows():
                    if 'andamentoData' in row and pd.notna(row['andamentoData']):
                        data_andamento = pd.to_datetime(row['andamentoData'])
                        desc_andamento = row.get('andamentoDescricao')

                        andamento_existente = session.query(Andamento).filter_by(processo_id=processo_db.id,
                                                                                 data=data_andamento,
                                                                                 descricao=desc_andamento).first()
                        if not andamento_existente:
                            session.add(
                                Andamento(processo_id=processo_db.id, data=data_andamento, descricao=desc_andamento))
                            print(f"    -> Andamento de '{data_andamento.date()}' salvo.")

                # Atualiza status do processo e pesquisa
                processo_db.dados_processo_encontrado = True

            pesquisa_db.status = 'CONCLUIDO'
            print(f"  Pesquisa {cod_pesquisa} marcada como CONCLUIDO.")

        session.commit()
        print("Importação concluída com sucesso!")

    except Exception as e:
        if session:
            session.rollback()
        print(f"ERRO CRÍTICO durante a importação: {e}")
    finally:
        if session:
            session.close()


if __name__ == '__main__':
    importar_de_excel_v5()
