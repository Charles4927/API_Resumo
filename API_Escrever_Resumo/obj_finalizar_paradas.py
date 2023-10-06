from datetime import datetime, timedelta
from obj_conexoes_bco_dados import Conexoes_SQL


class Finalizar_Paradas_Abertas:
    def __init__(self, tabela_sql_producao, tabela_sql_resumo):
        self.tabela_sql_producao = tabela_sql_producao
        self.tabela_sql_resumo = tabela_sql_resumo

        # Abrindo conexão com o banco de dados:
        conexao = Conexoes_SQL('cdtmes').obter_conexao()
        cursor = conexao.cursor()

        # consultando a tabela resumo e guardando todos os itens na variavel lista_todos_os_valores:
        comando_sql_pegar_todos_valores = ("SELECT data, minuto, producao, parada_aberta_data, parada_aberta_hora, parada_finalizada_data, parada_finalizada_hora FROM " + self.tabela_sql_resumo)
        cursor.execute(comando_sql_pegar_todos_valores)
        lista_todos_os_valores = cursor.fetchall()
        print("lista_todos_os_valores ult.5:", lista_todos_os_valores[-5:])

        # Pegando linhas para verificar se tem parada aberta e se dá para finalizar
        parada_finalizada_com_texto_aberta = [tupla for tupla in lista_todos_os_valores if 'aberta' in tupla]
        print("parada_finalizada_com_texto_aberta:", parada_finalizada_com_texto_aberta)
        print("len parada_finalizada_com_texto_aberta:", len(parada_finalizada_com_texto_aberta))

        # 1º Critério: Se a quantidade de "parada_finalizada_com_texto_aberta" é maior que 0.
        if len(parada_finalizada_com_texto_aberta) > 0:

            # Pegando agora a ultima linha do banco (seja preenchida ou não)
            ultima_parada_registrada_no_bco = lista_todos_os_valores[-1]
            print("ultima_parada_registrada_no_bco:", ultima_parada_registrada_no_bco)

            # Pegando o horario da "ultima_parada_registrada_no_bco"
            horario_finalizacao_ultima_parada_registrada_no_bco = ultima_parada_registrada_no_bco[-1]
            print("horario_finalizacao_ultima_parada_registrada_no_bco:", horario_finalizacao_ultima_parada_registrada_no_bco)

            # Se encontrar mais que 1 'aberta' em toda a 'tebela_resumo'
            if len([tupla for tupla in lista_todos_os_valores if 'aberta' in tupla]) > 1:

                # Gerando código para pegar tempo subtraído:
                data_para_subtrair = ultima_parada_registrada_no_bco[3]
                hora_para_subtrair = ultima_parada_registrada_no_bco[4]
                print("data_para_subtrair", data_para_subtrair)
                print("hora_para_subtrair", hora_para_subtrair)

                data_inicio = datetime.strptime(data_para_subtrair + ' ' + hora_para_subtrair, '%d/%m/%Y %H:%M:%S')
                data_fim = data_inicio - timedelta(seconds=30)
                data_subtraida = data_fim.strftime("%d/%m/%Y")
                hora_subtraida = data_fim.strftime("%H:%M:%S")
                print(data_subtraida)
                print(hora_subtraida)

                data_e_minuto_resumo_da_parada_finalizada_com_texto_aberta = parada_finalizada_com_texto_aberta[0][:2]
                data_e_minuto_resumo_da_ultima_parada_registrada_no_bco = ultima_parada_registrada_no_bco[:2]
                print("data_e_minuto_resumo_da_parada_finalizada_com_texto_aberta:", data_e_minuto_resumo_da_parada_finalizada_com_texto_aberta)
                print("data_e_minuto_resumo_da_ultima_parada_registrada_no_bco:", data_e_minuto_resumo_da_ultima_parada_registrada_no_bco)
                comando_sql1 = ("UPDATE " + self.tabela_sql_resumo + " SET parada_finalizada_data = '" + data_subtraida + "', parada_finalizada_hora = '" + hora_subtraida + "' WHERE data = '" + data_e_minuto_resumo_da_parada_finalizada_com_texto_aberta[0] + "' AND minuto = '" + data_e_minuto_resumo_da_parada_finalizada_com_texto_aberta[1] + "'")
                print("comando_sql1:", comando_sql1)
                cursor.execute(comando_sql1)
                cursor.commit()
            else:
                # Se ultima_parada_registrada_no_bco não contiver 'aberta' e se parada_aberta_data for diferente de vazio (Acho que precisaria rever isto)
                # if 'aberta' not in [elemento for elemento in ultima_parada_registrada_no_bco] and ultima_parada_registrada_no_bco[3] != '':
                
                # Hoje, 25/07/2023 estou mudando o segundo critério para se [2]"produção for maior que zero":
                if 'aberta' not in [elemento for elemento in ultima_parada_registrada_no_bco] and ultima_parada_registrada_no_bco[2] != '':
                    data_e_minuto_resumo_da_parada_finalizada_com_texto_aberta = parada_finalizada_com_texto_aberta[0][:2]
                    data_e_minuto_resumo_da_ultima_parada_registrada_no_bco = ultima_parada_registrada_no_bco[:2]
                    print("data_e_minuto_resumo_da_parada_finalizada_com_texto_aberta:", data_e_minuto_resumo_da_parada_finalizada_com_texto_aberta)
                    print("data_e_minuto_resumo_da_ultima_parada_registrada_no_bco:", data_e_minuto_resumo_da_ultima_parada_registrada_no_bco)
                    comando_sql1 = ("UPDATE " + self.tabela_sql_resumo + " SET parada_finalizada_data = '" + ultima_parada_registrada_no_bco[5] + "', parada_finalizada_hora = '" + ultima_parada_registrada_no_bco[6] + "' WHERE data = '" + data_e_minuto_resumo_da_parada_finalizada_com_texto_aberta[0] + "' AND minuto = '" + data_e_minuto_resumo_da_parada_finalizada_com_texto_aberta[1] + "'")
                    comando_sql2 = ("UPDATE " + self.tabela_sql_resumo + " SET parada_aberta_data = '', parada_aberta_hora = '', parada_finalizada_data = '', parada_finalizada_hora = '' WHERE data = '" + data_e_minuto_resumo_da_ultima_parada_registrada_no_bco[0] + "' AND minuto = '" + data_e_minuto_resumo_da_ultima_parada_registrada_no_bco[1] + "'")
                    cursor.execute(comando_sql1)
                    cursor.execute(comando_sql2)
                    cursor.commit()
                    print("comando_sql1:", comando_sql1)
                    print("comando_sql2:", comando_sql2)
                else:
                    pass

        # Só vou fechar após escrever no banco:
        cursor.close()
        conexao.close()


if __name__ == "__main__":
    Finalizar_Paradas_Abertas('luxor_producao', 'luxor_resumo')
    pass