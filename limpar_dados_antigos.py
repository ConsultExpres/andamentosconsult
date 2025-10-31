import sys
import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import boto3
from urllib.parse import urlparse

# --- IMPORTAÇÃO DE CLASSES E CONFIGURAÇÕES ---
try:
    from config_local import DATABASE_URL_REMOTE, S3_UPLOADER_ACCESS_KEY_ID, S3_UPLOADER_SECRET_ACCESS_KEY
except ImportError:
    print("ERRO CRÍTICO: O arquivo 'config_local.py' não foi encontrado ou está incompleto.")
    sys.exit(1)

try:
    from app import Cliente, Pesquisa, Processo, CapaProcesso, DocumentoInicial, Andamento, Parte, Advogado, \
        db as original_db
    from flask import Flask

    app = Flask(__name__)
except ImportError:
    print("ERRO CRÍTICO: Falha ao importar classes do app.py.")
    sys.exit(1)

# --- CONSTANTES ---
S3_BUCKET_NAME = "andamentosconsult"  # Seu bucket S3
S3_REGION = "us-east-2"  # Região do seu bucket (Ohio)
DIAS_PARA_LIMPEZA = 7  # Apaga dados entregues há mais de 7 dias


# --- FUNÇÃO DE CONEXÃO ---
def get_remote_session():
    """ Cria uma sessão de banco de dados conectada ao PostgreSQL do Render. """
    print(f"Conectando ao banco de dados remoto...")
    url = DATABASE_URL_REMOTE
    engine = create_engine(url)
    with app.app_context():
        original_db.metadata.reflect(engine,
                                     only=['cliente', 'pesquisa', 'processo', 'capaprocessp', 'documentoinicial',
                                           'andamento', 'parte', 'advogado'])
    Session = sessionmaker(bind=engine)
    return Session()


# --- FUNÇÃO PRINCIPAL DE LIMPEZA ---
def limpar_dados_antigos():
    print("Iniciando script de limpeza de dados antigos...")
    session = None

    # 1. Conectar ao S3
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=S3_UPLOADER_ACCESS_KEY_ID,
            aws_secret_access_key=S3_UPLOADER_SECRET_ACCESS_KEY,
            region_name=S3_REGION
        )
        print("Conectado ao S3 com sucesso.")
    except Exception as e:
        print(f"ERRO: Falha ao conectar no S3. Verifique as chaves S3_UPLOADER no config_local.py. {e}")
        return

    # 2. Conectar ao Banco de Dados
    try:
        session = get_remote_session()

        # 3. Define a data limite
        limite_data = datetime.datetime.utcnow() - datetime.timedelta(days=DIAS_PARA_LIMPEZA)
        print(f"Procurando dados entregues antes de: {limite_data.date()}")

        # 4. Encontra pesquisas ENTREGUES e ANTIGAS
        pesquisas_para_deletar = session.query(Pesquisa).filter(
            Pesquisa.status == 'ENTREGUE',
            Pesquisa.data_entrega < limite_data
        ).all()

        if not pesquisas_para_deletar:
            print("Nenhuma pesquisa antiga para limpar. Sistema está limpo.")
            return

        print(f"Encontradas {len(pesquisas_para_deletar)} pesquisas para limpeza...")

        # 5. Itera e deleta (S3 e Banco)
        for pesquisa in pesquisas_para_deletar:
            print(f"  Limpando Pesquisa ID: {pesquisa.id} (Entregue em: {pesquisa.data_entrega.date()})")

            for processo in pesquisa.processos:

                # Deleta Documentos (e arquivos S3)
                for doc in processo.documentos:
                    if doc.link_documento and 'amazonaws.com/' in doc.link_documento:
                        try:
                            # Extrai a 'Key' (nome do arquivo) da URL
                            file_key = doc.link_documento.split('amazonaws.com/')[1].split('?')[0]
                            print(f"    -> Deletando S3: {file_key}")
                            s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=file_key)
                        except Exception as s3_error:
                            print(f"    -> ERRO ao deletar S3: {s3_error}")
                    session.delete(doc)

                # Deleta dados associados
                for item in processo.partes: session.delete(item)
                for item in processo.advogados: session.delete(item)
                for item in processo.andamentos: session.delete(item)
                if processo.capa: session.delete(processo.capa)

                session.delete(processo)

            session.delete(pesquisa)
            print(f"  -> Pesquisa ID: {pesquisa.id} deletada do banco.")

        session.commit()
        print("Limpeza concluída com sucesso!")

    except Exception as e:
        if session:
            session.rollback()
        print(f"\nERRO CRÍTICO durante a limpeza: {e}")
    finally:
        if session:
            session.close()


if __name__ == '__main__':
    limpar_dados_antigos()