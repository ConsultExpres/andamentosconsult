import jwt
import time
import datetime
import os
from flask import Flask, request, jsonify, make_response, send_from_directory
from flask_sqlalchemy import SQLAlchemy
import boto3  # Adicionado para S3

# --- 1. CONFIGURAÇÃO INICIAL (ATUALIZADA PARA DEPLOY) ---
app = Flask(__name__)

# Lê a Chave Secreta do ambiente. Se não achar, usa a de dev.
APP_SECRET_KEY = os.environ.get('APP_SECRET_KEY', 'minha-chave-secreta-local-123')

# Lê a URL do Banco de Dados do ambiente (Ex: "postgresql://...")
DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    # Estamos em produção (na nuvem)
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
else:
    # Estamos em desenvolvimento (local)
    basedir = os.path.abspath(os.path.dirname(__file__))
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'api.db')

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


# --- 2. MODELOS (Tabelas do Banco - CORRIGIDO O MAPPING PARA POSTGRESQL) ---

class Cliente(db.Model):
    __tablename__ = 'cliente'
    id = db.Column(db.Integer, primary_key=True)
    nome_relacional = db.Column(db.String(80), unique=True, nullable=False)
    token_api = db.Column(db.String(120), nullable=False)
    pesquisas = db.relationship('Pesquisa', backref='cliente', lazy=True)


class Pesquisa(db.Model):
    __tablename__ = 'pesquisa'
    id = db.Column(db.Integer, primary_key=True)
    cliente_id = db.Column(db.Integer, db.ForeignKey('cliente.id'), nullable=False)
    instancia = db.Column(db.Integer)
    entregar_publicacoes = db.Column(db.Boolean, default=False)
    entregar_doc_iniciais = db.Column(db.Boolean, default=True)
    data_criacao = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    status = db.Column(db.String(50), default='PENDENTE')

    # TÓPICO 2: Adiciona coluna para limpeza de custos
    data_entrega = db.Column(db.DateTime, nullable=True)

    processos = db.relationship('Processo', backref='pesquisa', lazy=True)


class Processo(db.Model):
    __tablename__ = 'processo'
    id = db.Column(db.Integer, primary_key=True)
    pesquisa_id = db.Column(db.Integer, db.ForeignKey('pesquisa.id'), nullable=False)
    numero_processo = db.Column(db.String(100), nullable=False)
    dados_processo_encontrado = db.Column(db.Boolean, default=False)
    capa = db.relationship('CapaProcesso', backref='processo', uselist=False, lazy=True)
    documentos = db.relationship('DocumentoInicial', backref='processo', lazy=True)
    andamentos = db.relationship('Andamento', backref='processo', lazy=True)
    partes = db.relationship('Parte', backref='processo', lazy=True)
    advogados = db.relationship('Advogado', backref='processo', lazy=True)


class CapaProcesso(db.Model):
    # CORRIGIDO: Mapeia para o nome exato do erro ('capaprocessp')
    __tablename__ = 'capaprocessp'
    id = db.Column(db.Integer, primary_key=True)
    processo_id = db.Column(db.Integer, db.ForeignKey('processo.id'), unique=True, nullable=False)
    valor_causa = db.Column(db.Float, nullable=True)
    classe_cnj = db.Column(db.String(200), default="")
    area = db.Column(db.String(100), default="")


class DocumentoInicial(db.Model):
    # CORRIGIDO: Mapeia para o nome exato do erro ('documentoinicial')
    __tablename__ = 'documentoinicial'
    id = db.Column(db.Integer, primary_key=True)
    processo_id = db.Column(db.Integer, db.ForeignKey('processo.id'), nullable=False)
    link_documento = db.Column(db.String(500), nullable=True)
    documento_encontrado = db.Column(db.Boolean, default=False)
    doc_peticao_inicial = db.Column(db.Boolean, default=False)


class Andamento(db.Model):
    __tablename__ = 'andamento'
    id = db.Column(db.Integer, primary_key=True)
    processo_id = db.Column(db.Integer, db.ForeignKey('processo.id'), nullable=False)
    data = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    descricao = db.Column(db.Text)


class Parte(db.Model):
    __tablename__ = 'parte'
    id = db.Column(db.Integer, primary_key=True)
    processo_id = db.Column(db.Integer, db.ForeignKey('processo.id'), nullable=False)
    tipo = db.Column(db.String(100))
    nome = db.Column(db.String(500))


class Advogado(db.Model):
    __tablename__ = 'advogado'
    id = db.Column(db.Integer, primary_key=True)
    processo_id = db.Column(db.Integer, db.ForeignKey('processo.id'), nullable=False)
    tipo = db.Column(db.String(100))
    nome = db.Column(db.String(500))
    oab = db.Column(db.String(50), nullable=True)


# --- 3. HELPER (Função para validar o token) ---
def validar_token():
    token_recebido = request.headers.get('Authorization')
    if not token_recebido:
        return None, (jsonify({"erro": "Header 'Authorization' ausente"}), 401)
    try:
        payload = jwt.decode(token_recebido, APP_SECRET_KEY, algorithms=["HS256"])
        return payload, None
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None, (jsonify({"erro": "Token invalido ou expirado"}), 401)


# --- 4. ENDPOINTS DA API ---

@app.route('/WebApiDiscoveryFullV2/api/DiscoveryFull/autenticaAPI', methods=['POST'])
def autentica_api():
    try:
        dados = request.get_json()
        nome_relacional = dados.get('nomeRelacional')
        token_cliente = dados.get('token')
        cliente = Cliente.query.filter_by(
            nome_relacional=nome_relacional,
            token_api=token_cliente
        ).first()
        if cliente:
            payload = {
                'iat': int(time.time()), 'nbf': int(time.time()),
                'exp': int(time.time()) + 1800,
                'id_cliente_interno': cliente.id,
                'nomeRelacional': cliente.nome_relacional
            }
            token_jwt = jwt.encode(payload, APP_SECRET_KEY, algorithm="HS256")
            response = make_response(token_jwt, 200)
            response.mimetype = "text/plain"
            return response
        else:
            return jsonify({"erro": "Credenciais invalidas"}), 401
    except Exception as e:
        return jsonify({"erro": "Formato invalido"}), 400


@app.route('/WebApiDiscoveryFullV2/api/DiscoveryFull/CadastraPesquisa_NumProcessos', methods=['POST'])
def cadastra_pesquisa():
    payload, erro = validar_token()
    if erro: return erro
    try:
        id_cliente_logado = payload['id_cliente_interno']
        dados_pesquisa = request.get_json()
        nova_pesquisa = Pesquisa(
            cliente_id=id_cliente_logado,
            instancia=dados_pesquisa.get('instancia'),
            status='PENDENTE'
        )
        db.session.add(nova_pesquisa)
        db.session.commit()
        for num_proc in dados_pesquisa.get('listaNumProcessos', []):
            db.session.add(Processo(pesquisa_id=nova_pesquisa.id, numero_processo=num_proc))
        db.session.commit()
        return jsonify({"codPesquisa": nova_pesquisa.id}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"erro": "Erro interno ao processar"}), 500


@app.route('/WebApiDiscoveryFullV2/api/DiscoveryFull/buscaDadosResultadoPesquisa', methods=['POST'])
def busca_dados_capa():
    payload, erro = validar_token()
    if erro: return erro
    try:
        dados = request.get_json()
        cod_pesquisa = dados.get('codPesquisa')
        pesquisa = Pesquisa.query.get(cod_pesquisa)
        if not pesquisa: return jsonify({"erro": "codPesquisa nao encontrado"}), 404
        if pesquisa.cliente_id != payload['id_cliente_interno']:
            return jsonify({"erro": "Acesso negado a esta pesquisa"}), 403

        # TÓPICO 1: Lógica de Status
        if pesquisa.status in ('PENDENTE', 'PROCESSANDO'):
            return jsonify({"status": "processando",
                            "mensagem": "Os resultados desta pesquisa ainda estão sendo processados."}), 202

        # Se o status for CONCLUIDO, muda para ENTREGUE e salva a data
        if pesquisa.status == 'CONCLUIDO':
            pesquisa.status = 'ENTREGUE'
            pesquisa.data_entrega = datetime.datetime.utcnow()
            db.session.add(pesquisa)
            db.session.commit()

        # (Se o status for 'ENTREGUE', apenas continua e retorna os dados)

        resposta_final = []
        for proc in pesquisa.processos:
            capa = proc.capa
            partes_json = []
            for parte in proc.partes:
                partes_json.append({"tipo": parte.tipo, "nome": parte.nome})
            advogados_json = []
            for adv in proc.advogados:
                advogados_json.append({"tipo": adv.tipo, "nome": adv.nome, "oab": adv.oab})
            capa_json = {
                "siglaTribunal": None, "relator": None, "dataDistribuicao": None,
                "dataAutuacao": None, "orgaoJulgador": None, "segmento": "", "uf": "",
                "unidadeOrigem": None, "statusProcesso": None, "dataArquivamento": None,
                "ramoDireito": None, "eSegredoJustica": None,
                "classeCnj": capa.classe_cnj if capa else "",
                "area": capa.area if capa else "",
            }
            processo_json = {
                "codProcesso": proc.id, "numeroProcessoFormatado": proc.numero_processo, "numeroNaoCnj": None,
                "instancia": pesquisa.instancia, "valorCausa": capa.valor_causa if capa else None, "assuntos": None,
                "capaProcesso": capa_json, "partes": partes_json, "advogados": advogados_json,
                "dadosProcessoEncontrado": proc.dados_processo_encontrado
            }
            resposta_final.append(processo_json)
        return jsonify(resposta_final), 200
    except Exception as e:
        db.session.rollback()
        print(f"Erro em /buscaDadosResultadoPesquisa: {e}")
        return jsonify({"erro": "Erro interno ao processar"}), 500


@app.route('/WebApiDiscoveryFullV2/api/DiscoveryFull/buscaDadosDocIniciaisPesquisa', methods=['POST'])
def busca_docs_iniciais():
    """ Endpoint 4: Recupera Cópia Integral (AGORA COM LINK PRÉ-ASSINADO SEGURO) """
    payload, erro = validar_token()
    if erro: return erro
    try:
        dados = request.get_json()
        cod_pesquisa = dados.get('codPesquisa')
        pesquisa = Pesquisa.query.get(cod_pesquisa)
        if not pesquisa: return jsonify({"erro": "codPesquisa nao encontrado"}), 404
        if pesquisa.cliente_id != payload['id_cliente_interno']:
            return jsonify({"erro": "Acesso negado a esta pesquisa"}), 403

        # TÓPICO 1: Lógica de Status
        if pesquisa.status in ('PENDENTE', 'PROCESSANDO'):
            return jsonify({"status": "processando",
                            "mensagem": "Os resultados desta pesquisa ainda estão sendo processados."}), 202

        if pesquisa.status == 'CONCLUIDO':
            pesquisa.status = 'ENTREGUE'
            pesquisa.data_entrega = datetime.datetime.utcnow()
            db.session.add(pesquisa)
            db.session.commit()

        # (Se o status for 'ENTREGUE', apenas continua e retorna os dados)

        # --- CONEXÃO S3 E GERAÇÃO DE LINK ---
        aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        S3_BUCKET_NAME = "andamentosconsult"  # Seu bucket
        S3_REGION = "us-east-2"  # Região do seu bucket (Ohio)

        if aws_access_key and aws_secret_key:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=S3_REGION
            )
        else:
            s3_client = None

        resposta_final = []
        for proc in pesquisa.processos:
            for doc in proc.documentos:
                signed_url = doc.link_documento

                if s3_client and doc.link_documento and 'amazonaws.com/' in doc.link_documento:
                    try:
                        file_key = doc.link_documento.split('amazonaws.com/')[1].split('?')[0]
                        signed_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': S3_BUCKET_NAME, 'Key': file_key},
                            ExpiresIn=300  # Válido por 5 minutos
                        )
                    except Exception as s3_error:
                        print(f"Erro ao gerar link S3: {s3_error}")
                        signed_url = doc.link_documento

                resposta_final.append({
                    "codDocIniciais": doc.id, "codPesquisa": pesquisa.id, "codProcesso": proc.id,
                    "linkDocumentosIniciais": signed_url,
                    "docPeticaoInicial": doc.doc_peticao_inicial,
                    "documentoEncontrado": doc.documento_encontrado
                })
        return jsonify(resposta_final), 200
    except Exception as e:
        db.session.rollback()
        print(f"Erro em /buscaDadosDocIniciaisPesquisa: {e}")
        return jsonify({"erro": "Erro interno ao processar"}), 500


@app.route('/WebApiDiscoveryFullV2/api/DiscoveryFull/buscaAndamentosProcesso', methods=['POST'])
def busca_andamentos():
    payload, erro = validar_token()
    if erro: return erro
    try:
        dados = request.get_json()
        num_processo = dados.get('numeroProcesso')
        processo = Processo.query.filter_by(numero_processo=num_processo).first()
        if not processo: return jsonify({"erro": "Processo nao encontrado"}), 404
        if processo.pesquisa.cliente_id != payload['id_cliente_interno']:
            return jsonify({"erro": "Acesso negado a este processo"}), 403

        # TÓPICO 1: Lógica de Status (aplicada à pesquisa-mãe)
        pesquisa = processo.pesquisa
        if pesquisa.status in ('PENDENTE', 'PROCESSANDO'):
            return jsonify({"status": "processando",
                            "mensagem": "Os resultados desta pesquisa ainda estão sendo processados."}), 202

        if pesquisa.status == 'CONCLUIDO':
            pesquisa.status = 'ENTREGUE'
            pesquisa.data_entrega = datetime.datetime.utcnow()
            db.session.add(pesquisa)
            db.session.commit()

        # (Se o status for 'ENTREGUE', apenas continua e retorna os dados)

        resposta_final = []
        for andamento in processo.andamentos:
            resposta_final.append({"data": andamento.data.isoformat(), "andamento": andamento.descricao})
        return jsonify(resposta_final), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"erro": "Erro interno ao processar"}), 500


# --- ROTA TEMPORÁRIA DE SETUP (CRIA TABELAS E CLIENTES) ---
@app.route('/admin/setup-database/criaaiconsult2025')
def setup_database():
    """
    Endpoint de admin. CRIA AS TABELAS no PostgreSQL e POPULA clientes iniciais.
    """
    try:
        with app.app_context():
            # --- PASSO 1: CRIAR AS TABELAS (e colunas novas) ---
            db.create_all()

            # --- PASSO 2: POPULAR O CLIENTE 1 e 2 ---
            cliente1_existe = Cliente.query.filter_by(nome_relacional="CRIAAI").first()
            if not cliente1_existe:
                cliente_teste = Cliente(nome_relacional="CRIAAI", token_api="senha")
                db.session.add(cliente_teste)

            cliente2_existe = Cliente.query.filter_by(nome_relacional="CRY2").first()
            if not cliente2_existe:
                cliente_cry2 = Cliente(nome_relacional="CRY2", token_api="outra-senha-secreta-456")
                db.session.add(cliente_cry2)

            db.session.commit()
            return jsonify({"status": "sucesso", "mensagem": "Tabelas criadas/atualizadas e banco populado."}), 200

    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "erro", "mensagem": str(e)}), 500


# --- FIM DA ROTA DE SETUP ---


# --- 5. RODE O SERVIDOR ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)