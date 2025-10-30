import pandas as pd
from app import app, db, Pesquisa, Processo


def exportar_para_excel():
    print("Iniciando exportação de pesquisas pendentes...")

    # Lista para guardar os dados para o Excel
    dados_para_exportar = []

    with app.app_context():
        # 1. Busca todas as pesquisas com status 'PENDENTE'
        pesquisas_pendentes = Pesquisa.query.filter_by(status='PENDENTE').all()

        if not pesquisas_pendentes:
            print("Nenhuma pesquisa pendente encontrada.")
            return

        print(f"Encontradas {len(pesquisas_pendentes)} pesquisas pendentes.")

        for pesquisa in pesquisas_pendentes:
            # 2. Para cada pesquisa, pega os números de processo
            for processo in pesquisa.processos:
                dados_para_exportar.append({
                    "codPesquisa": pesquisa.id,
                    "numeroProcesso": processo.numero_processo,
                    "instancia": pesquisa.instancia
                })

            # 3. Atualiza o status da pesquisa para 'PROCESSANDO'
            pesquisa.status = 'PROCESSANDO'

        # 4. Salva as mudanças de status no banco
        db.session.commit()

    # 5. Cria um DataFrame com os dados e salva em Excel
    if dados_para_exportar:
        df = pd.DataFrame(dados_para_exportar)
        df.to_excel("pendentes.xlsx", index=False, engine='openpyxl')
        print(f"Sucesso! 'pendentes.xlsx' criado com {len(df)} processos.")
    else:
        print("Nenhum processo encontrado nas pesquisas pendentes.")


if __name__ == '__main__':
    exportar_para_excel()