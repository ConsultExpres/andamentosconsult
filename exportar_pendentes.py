import pandas as pd
import sys
import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# --- IMPORTAÇÃO DE CLASSES E CONFIGURAÇÕES ---
try:
    from config_local import DATABASE_URL_REMOTE
except ImportError:
    print("ERRO CRÍTICO: O arquivo 'config_local.py' não foi encontrado. Crie e cole sua DATABASE_URL_REMOTE.")
    sys.exit(1)

try:
    from app import Cliente, Pesquisa, Processo, db as original_db, CapaProcesso, DocumentoInicial, Andamento, Parte, \
        Advogado
    from flask import Flask

    app = Flask(__name__)
except ImportError:
    print("ERRO CRÍTICO: Falha ao importar classes do app.py. O app.py está na pasta raiz?")
    print("Por favor, certifique-se de que o app.py foi salvo com o Código 1 que eu forneci.")
    sys.exit(1)


# --- FUNÇÕES HELPER PARA CONEXÃO E DIAGNÓSTICO ---

def get_remote_session():
    """ Cria uma sessão de banco de dados conectada ao PostgreSQL do Render. """
    print(f"Conectando ao banco de dados remoto...")

    url = DATABASE_URL_REMOTE

    try:
        engine = create_engine(url)
    except Exception as e:
        print(f"ERRO: Falha ao criar o motor de conexão. URL correta? {e}")
        raise

    # Tentativa 2: Fazer o mapeamento dos modelos (que foi o erro anterior)
    try:
        with app.app_context():
            # CORREÇÃO: Informa ao SQLAlchemy os nomes exatos das tabelas no PostgreSQL
            original_db.metadata.reflect(engine, only=[
                'cliente',
                'pesquisa',
                'processo',
                'capaprocessp',  # Nome exato do erro
                'documentoinicial',  # Nome exato do erro
                'andamento',
                'parte',
                'advogado'
            ])
    except Exception as e:
        print(f"ERRO: Falha no mapeamento (Reflect). O Deploy no Render foi concluído?")
        print(
            f"Verifique se os nomes das tabelas no app.py (__tablename__) correspondem exatamente a 'capaprocessp' e 'documentoinicial'")
        print(f"Erro detalhado: {e}")
        raise

    Session = sessionmaker(bind=engine)
    return Session()


def exportar_para_excel():
    session = None
    try:
        session = get_remote_session()
        print("Iniciando diagnóstico de status...")

        # 1. Tenta buscar pesquisas com o status 'PENDENTE' (o status esperado)
        pesquisas_pendentes_direto = session.query(Pesquisa).filter_by(status='PENDENTE').all()

        if pesquisas_pendentes_direto:
            print(
                f"SUCESSO: Encontradas {len(pesquisas_pendentes_direto)} pesquisas com o status 'PENDENTE'. Exportando...")

            # --- LÓGICA DE EXPORTAÇÃO ---
            dados_para_exportar = []
            for pesquisa in pesquisas_pendentes_direto:
                for processo in pesquisa.processos:
                    dados_para_exportar.append({
                        "codPesquisa": pesquisa.id,
                        "numeroProcesso": processo.numero_processo,
                        "instancia": pesquisa.instancia
                    })
                # Marca como PROCESSANDO
                pesquisa.status = 'PROCESSANDO'
                session.add(pesquisa)

            session.commit()

            if dados_para_exportar:
                df = pd.DataFrame(dados_para_exportar)
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"pendentes_{timestamp}.xlsx"
                df.to_excel(filename, index=False, engine='openpyxl')
                print(f"SUCESSO: Relatório '{filename}' criado. Status alterado para PROCESSANDO.")
            return

        print("\n--- DIAGNÓSTICO DE STATUS ---\n")
        print("AVISO: Nenhuma pesquisa PENDENTE encontrada. Buscando TODOS os status para diagnóstico...")

        # 2. Se a busca por 'PENDENTE' falhar, buscamos TODOS os itens
        todas_as_pesquisas = session.query(Pesquisa).all()

        if not todas_as_pesquisas:
            print("NÃO ENCONTRADO: Nenhuma pesquisa (em qualquer status) foi encontrada no banco de dados.")
            return

        # Imprime o status real de cada item no banco
        status_contagem = {}
        for pesquisa in todas_as_pesquisas:
            status = pesquisa.status
            status_contagem[status] = status_contagem.get(status, 0) + 1
            print(f"PESQUISA ID: {pesquisa.id} | STATUS REAL: {status}")

        print("\nRESUMO DO DIAGNÓSTICO:")
        for status, count in status_contagem.items():
            print(f"STATUS ENCONTRADO: '{status}' ({count} itens)")

    except Exception as e:
        if session:
            session.rollback()
        print(f"\nERRO CRÍTICO durante a conexão/execução: {e}")
    finally:
        if session:
            session.close()


if __name__ == '__main__':
    # Antes de rodar, crie um pedido usando a Requisição 2 no teste.http (na nuvem)
    exportar_para_excel()
