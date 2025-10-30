import pandas as pd
from app import app, db, Pesquisa, Processo, CapaProcesso, DocumentoInicial, Andamento, Parte, Advogado
import re  # Usaremos para extrair a OAB

# --- IMPORTANTE: Configure a URL base da sua API ---
BASE_URL_API = "http://localhost:8080"


# Função helper para extrair OAB
def extrair_oab(texto):
    match = re.search(r'\((.*?)\)', texto)
    if match:
        oab = match.group(1)
        nome = texto.replace(f"({oab})", "").strip()
        return nome, oab
    return texto.strip(), None


def importar_de_excel_v4():
    print("Iniciando importação de resultados (v4 - Formato Simplificado)...")

    try:
        df = pd.read_excel("resultados.xlsx")
    except FileNotFoundError:
        print("ERRO: Arquivo 'resultados.xlsx' não encontrado.")
        return
    except Exception as e:
        print(f"ERRO ao ler o Excel: {e}")
        return

    with app.app_context():
        # Agrupa por 'codPesquisa'
        for cod_pesquisa, group in df.groupby('codPesquisa'):

            print(f"Processando codPesquisa: {cod_pesquisa}...")

            pesquisa_db = Pesquisa.query.get(int(cod_pesquisa))
            if not pesquisa_db:
                print(f"  Aviso: codPesquisa {cod_pesquisa} não encontrado. Pulando.")
                continue

            # Dentro de cada pesquisa, agrupa por 'numeroProcesso'
            for num_processo, proc_group in group.groupby('numeroProcesso'):

                processo_db = Processo.query.filter_by(
                    pesquisa_id=pesquisa_db.id,
                    numero_processo=num_processo
                ).first()

                if not processo_db:
                    print(f"  Aviso: Processo {num_processo} não encontrado. Pulando.")
                    continue

                print(f"  Atualizando processo: {num_processo} (ID: {processo_db.id})")

                # --- 1. Pega os dados da CAPA, PDF, PARTES, ADVOGADOS ---
                # Pega a *primeira* linha do grupo que tenha esses dados
                primeira_linha = proc_group.iloc[0]

                # --- 2. Salva a CAPA (se não existir) ---
                capa_existente = CapaProcesso.query.filter_by(processo_id=processo_db.id).first()
                if not capa_existente and pd.notna(primeira_linha.get('valorCausa')):
                    nova_capa = CapaProcesso(
                        processo_id=processo_db.id,
                        valor_causa=primeira_linha.get('valorCausa'),
                        classe_cnj=primeira_linha.get('classeCNJ'),
                        area=primeira_linha.get('area')
                    )
                    db.session.add(nova_capa)
                    print(f"    -> Capa criada.")

                # --- 3. Salva o PDF (se não existir) ---
                pdf_nome = primeira_linha.get('pdfNomeArquivo')
                if pd.notna(pdf_nome):
                    link_pdf = f"{BASE_URL_API}/documentos/{pdf_nome}"
                    doc_existente = DocumentoInicial.query.filter_by(processo_id=processo_db.id,
                                                                     link_documento=link_pdf).first()
                    if not doc_existente:
                        novo_doc = DocumentoInicial(
                            processo_id=processo_db.id,
                            link_documento=link_pdf,
                            documento_encontrado=True
                        )
                        db.session.add(novo_doc)
                        print(f"    -> Documento '{pdf_nome}' salvo.")

                # --- 4. Salva as PARTES (se não existirem) ---
                partes_texto = primeira_linha.get('partes')
                if pd.notna(partes_texto):
                    partes_lista = str(partes_texto).split('|')  # Separa por |
                    for p in partes_lista:
                        try:
                            tipo, nome = p.split(':', 1)  # Separa por :
                            parte_existente = Parte.query.filter_by(processo_id=processo_db.id, tipo=tipo.strip(),
                                                                    nome=nome.strip()).first()
                            if not parte_existente:
                                db.session.add(Parte(processo_id=processo_db.id, tipo=tipo.strip(), nome=nome.strip()))
                                print(f"    -> Parte '{nome.strip()}' salva.")
                        except:
                            print(f"    -> ERRO: Formato inválido na coluna 'partes': {p}")

                # --- 5. Salva os ADVOGADOS (se não existirem) ---
                advs_texto = primeira_linha.get('advogados')
                if pd.notna(advs_texto):
                    advs_lista = str(advs_texto).split('|')  # Separa por |
                    for a in advs_lista:
                        try:
                            tipo, nome_oab = a.split(':', 1)
                            nome, oab = extrair_oab(nome_oab)  # Helper para pegar a OAB
                            adv_existente = Advogado.query.filter_by(processo_id=processo_db.id, tipo=tipo.strip(),
                                                                     nome=nome).first()
                            if not adv_existente:
                                db.session.add(
                                    Advogado(processo_id=processo_db.id, tipo=tipo.strip(), nome=nome, oab=oab))
                                print(f"    -> Advogado '{nome}' salvo.")
                        except:
                            print(f"    -> ERRO: Formato inválido na coluna 'advogados': {a}")

                # --- 6. Salva os ANDAMENTOS (itera em TODAS as linhas) ---
                for _, row in proc_group.iterrows():
                    if 'andamentoData' in row and pd.notna(row['andamentoData']):
                        data_andamento = pd.to_datetime(row['andamentoData'])
                        desc_andamento = row.get('andamentoDescricao')

                        # Verifica se o andamento já existe
                        andamento_existente = Andamento.query.filter_by(processo_id=processo_db.id, data=data_andamento,
                                                                        descricao=desc_andamento).first()
                        if not andamento_existente:
                            db.session.add(
                                Andamento(processo_id=processo_db.id, data=data_andamento, descricao=desc_andamento))
                            print(f"    -> Andamento de '{data_andamento.date()}' salvo.")

                processo_db.dados_processo_encontrado = True

            pesquisa_db.status = 'CONCLUIDO'
            print(f"  Pesquisa {cod_pesquisa} marcada como CONCLUIDO.")

        try:
            db.session.commit()
            print("Importação (v4) concluída com sucesso!")
        except Exception as e:
            db.session.rollback()
            print(f"ERRO ao salvar no banco: {e}")


if __name__ == '__main__':
    # Importante: O nome da função mudou
    importar_de_excel_v4()