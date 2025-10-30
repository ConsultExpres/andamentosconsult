import pandas as pd
import sys
import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

# --- IMPORTAÇÃO DE CLASSES E CONFIGURAÇÕES ---
# Importa a URL de conexão do arquivo de configuração local
try:
    from config_local import DATABASE_URL_REMOTE
except ImportError:
    print("ERRO CRÍTICO: O arquivo 'config_local.py' não foi encontrado. Crie e cole sua DATABASE_URL_REMOTE.")
    sys.exit(1)

# Importa as classes de modelo (Pesquisa, Processo, etc.) do app.py
try:
    from app import Cliente, Pesquisa, Processo, db as original_db
    from flask import Flask

    app = Flask(__name__)
except ImportError:
    print("ERRO CRÍTICO: Falha ao importar classes do app.py. O app.py está na pasta raiz?")
    sys.exit(1)


# --- FUNÇÕES HELPER PARA CONEXÃO E DIAGNÓSTICO ---

def get_remote_session():
    """ Cria uma sessão de banco de dados conectada ao PostgreSQL do Render. """
    print(f"Conectando ao banco de dados remoto...")

    # A sua URL COMPLETA. Não é mais necessário o ajuste 'postgres://' -> 'postgresql://' pois a URL já está correta.
    url = DATABASE_URL_REMOTE

    engine = create_engine(url)

    # Garante que os modelos sejam reconhecidos pelo motor de conexão
    with app.app_context():
        # A reflexão é usada aqui para que os modelos do app.py sejam mapeados para a engine remota
        original_db.metadata.reflect(engine,
                                     only=['cliente', 'pesquisa', 'processo', 'capaprocessp', 'documentoinicial',
                                           'andamento', 'parte', 'advogado'])

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

            # --- LÓGICA DE EXPORTAÇÃO (SE TUDO ESTIVER CORRETO) ---
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
