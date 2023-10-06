from obj_finalizar_paradas import Finalizar_Paradas_Abertas
from obj_conexoes_bco_dados import Conexoes_SQL


class Redirecionar_Temporarios:
    def __init__(self, tabela_sql_producao, tabela_sql_resumo):
        self.tabela_sql_producao = tabela_sql_producao
        self.tabela_sql_resumo = tabela_sql_resumo

        # lista_maquinas = ['LX-01', 'LR-04', 'LR-05']

        # Abrindo conexão no SQL:
        conexao = Conexoes_SQL('cdtmes').obter_conexao()
        cursor = conexao.cursor()

        # Pegando todos os valores registrados em "temporarios":
        comando_pegar_temporarios = (f"SELECT temporarios FROM {self.tabela_sql_resumo} WHERE temporarios is not null")
        cursor.execute(comando_pegar_temporarios)
        valores_temporarios = cursor.fetchall()
        print("valores_temporarios:", valores_temporarios)

        # Se tiver valores registrados em "temporarios" recodificar os dados e dividir em familias:
        if len(valores_temporarios) > 0:

            # Pegando todos os valores (data, minuto) registrados na tabela resumo:
            comando_pegar_valores = (f"SELECT data, minuto FROM {self.tabela_sql_resumo}")
            cursor.execute(comando_pegar_valores)
            valores_data_minuto_ultimos_960 = cursor.fetchall()[-960:]
            # print("valores_data_minuto_ultimos_960:", valores_data_minuto_ultimos_960)

            # Decodificando os dados temporarios:
            dados_recodificados = []
            for item in valores_temporarios:
                dados_recodificados.append(str(item)[2:-4].split('•'))
            # print("dados_recodificados:", dados_recodificados)

        # Lista lista_apontamento_perda
            lista_apontamento_perda = [elemento for elemento in dados_recodificados if elemento[0] == 'apontamento_perda']
            print("lista_apontamento_perda:", lista_apontamento_perda)
            # Se lista_apontamento_perda contiver item:
            if len(lista_apontamento_perda) > 0:
                for indice in lista_apontamento_perda:
                    # gerando data,minuto do indice:
                    data_minuto_indice = []
                    data_minuto_indice.append(indice[1])
                    data_minuto_indice.append(indice[2][:5])
                    data_minuto_indice = tuple(data_minuto_indice)
                    print("data_minuto_indice:", data_minuto_indice)

                    # Se data,minuto do indice contiver nos 960
                    if str(data_minuto_indice) in str(valores_data_minuto_ultimos_960):

                        # # Comando para pegar a ordem e operador
                        comando_SELECT_OP_Operador = (f"SELECT ordem_de_producao, operador FROM {tabela_sql_resumo} Where data='{data_minuto_indice[0][0]}' and minuto='{data_minuto_indice[0][1]}'")
                        cursor.execute(comando_SELECT_OP_Operador)
                        op_operador = cursor.fetchall()
                        print("op_operador:", op_operador)
                        ordem = op_operador[0][0]
                        operador = op_operador[0][1]
                        print("ordem:", ordem)
                        print("operador:", operador)

                        try:
                            comando_INSERT_apontamento_perda = (f"INSERT INTO {self.tabela_sql_resumo} (data, minuto, perdas, perda_motivo, ordem_de_producao, operador) VALUES ('{indice[1]}', '{indice[2]}', '{indice[3]}', '{indice[4]}', '{ordem}', '{operador}')")
                            # print("comando_INSERT_apontamento_perda:", comando_INSERT_apontamento_perda)
                            comando_DELETE_temporarios_perda = (f"DELETE FROM {self.tabela_sql_resumo} WHERE temporarios LIKE '{indice[0]}•{indice[1]}•{indice[2]}%'")
                            # print("comando_DELETE_temporarios_perda:", comando_DELETE_temporarios_perda)
                            cursor.execute(comando_INSERT_apontamento_perda)
                            cursor.commit()
                            cursor.execute(comando_DELETE_temporarios_perda)
                            cursor.commit()
                        except:
                            # print("Deu erro de gravação!")
                            pass

        # Lista lista_op_operador
            lista_op_operador = [elemento for elemento in dados_recodificados if elemento[0] == 'op_operador']
            print("lista_op_operador:", lista_op_operador)
            # Se lista_op_operador contiver item:
            if len(lista_op_operador) > 0:
                for indice in lista_op_operador:
                    # gerando data,minuto do indice:
                    data_minuto_indice = []
                    data_minuto_indice.append(indice[1])
                    data_minuto_indice.append(indice[2][:5])
                    data_minuto_indice = tuple(data_minuto_indice)
                    print("data_minuto_indice:", data_minuto_indice)

                    # Se data,minuto do indice contiver nos 960
                    if str(data_minuto_indice) in str(valores_data_minuto_ultimos_960):
                        try:
                            comando_UPDATE_op_operador = (f"UPDATE {self.tabela_sql_resumo} SET ordem_de_producao = '{indice[4]}', operador = '{indice[3]}' WHERE data = '{indice[1]}' AND minuto = '{indice[2]}'")
                            comando_DELETE_op_operador = (f"DELETE FROM {self.tabela_sql_resumo} WHERE temporarios LIKE '{indice[0]}•{indice[1]}•{indice[2]}%'")
                            print("comando_UPDATE_op_operador:", comando_UPDATE_op_operador)
                            print("comando_DELETE_op_operador:", comando_DELETE_op_operador)
                            cursor.execute(comando_UPDATE_op_operador)
                            cursor.commit()
                            cursor.execute(comando_DELETE_op_operador)
                            cursor.commit()
                        except:
                            # print("Deu erro de gravação!")
                            pass

        # Lista lista_setup_INICIADO
            lista_setup_iniciado = [elemento for elemento in dados_recodificados if elemento[0] == 'setup_iniciado']
            # print("lista_setup_iniciado:", lista_setup_iniciado)
            # Se lista_setup_iniciado contiver item:
            if len(lista_setup_iniciado) > 0:
                for indice in lista_setup_iniciado:
                    # gerando data,minuto do indice:
                    data_minuto_indice = []
                    data_minuto_indice.append(indice[1])
                    data_minuto_indice.append(indice[2][:5])
                    data_minuto_indice = tuple(data_minuto_indice)
                    # print("data_minuto_indice:", data_minuto_indice)

                    # Se data,minuto do indice contiver nos 960
                    if str(data_minuto_indice) in str(valores_data_minuto_ultimos_960):
                        try:
                            comando_UPDATE_setup_iniciado = (f"UPDATE {self.tabela_sql_resumo} SET setup = 'setup_iniciado', parada_aberta_data = '{indice[1]}', parada_aberta_hora = '{indice[2]}', parada_finalizada_data = '{indice[1]}', parada_finalizada_hora = '{indice[2]}' WHERE data = '{indice[1]}' AND minuto = '{indice[2][:5]}'")
                            # print("comando_UPDATE_setup_iniciado:", comando_UPDATE_setup_iniciado)
                            cursor.execute(comando_UPDATE_setup_iniciado)
                            cursor.commit()

                            # Usa o obj_finalizar_paradas(abertas)
                            Finalizar_Paradas_Abertas(tabela_sql_producao, tabela_sql_resumo)

                            # Como o obj_finalizar_paradas(abertas) apaga a ultima linha escrita no banco, vai escrever novamente só que como 'aberta':
                            comando_UPDATE_2_setup_iniciado = (f"UPDATE {self.tabela_sql_resumo} SET parada_aberta_data = '{indice[1]}', parada_aberta_hora = '{indice[2]}', parada_finalizada_data = 'aberta', parada_finalizada_hora = 'aberta', parada_descricao = '{indice[4]}' WHERE data = '{indice[1]}' AND minuto = '{indice[2][:5]}'")
                            # print("comando_UPDATE_2_setup_iniciado:", comando_UPDATE_2_setup_iniciado)
                            cursor.execute(comando_UPDATE_2_setup_iniciado)
                            cursor.commit()

                            # Deletando o arquivo temporario:
                            comando_DELETE_setup_iniciado = (f"DELETE FROM {self.tabela_sql_resumo} WHERE temporarios LIKE '{indice[0]}•{indice[1]}•{indice[2][:5]}%'")
                            # print("comando_DELETE_setup_iniciado:", comando_DELETE_setup_iniciado)
                            cursor.execute(comando_DELETE_setup_iniciado)
                            cursor.commit()

                        except:
                            # print("Deu erro de gravação!")
                            pass
                        
        # Lista lista_setup_ENCERRADO
            lista_setup_encerrado = [elemento for elemento in dados_recodificados if elemento[0] == 'setup_encerrado']
            # print("lista_setup_encerrado:", lista_setup_encerrado)
            # Se lista_setup_encerrado contiver item:
            if len(lista_setup_encerrado) > 0:
                for indice in lista_setup_encerrado:
                    # gerando data,minuto do indice:
                    data_minuto_indice = []
                    data_minuto_indice.append(indice[1])
                    data_minuto_indice.append(indice[2][:5])
                    data_minuto_indice = tuple(data_minuto_indice)
                    # print("data_minuto_indice:", data_minuto_indice)

                    # Se data,minuto do indice contiver nos 960
                    if str(data_minuto_indice) in str(valores_data_minuto_ultimos_960):
                        try:
                            # Trocar INSERT por UPDATE:
                            comando_UPDATE_setup_encerrado = (f"UPDATE {self.tabela_sql_resumo} SET setup = 'setup_encerrado', parada_aberta_data = '{indice[1]}', parada_aberta_hora = '{indice[2]}', parada_finalizada_data = '{indice[1]}', parada_finalizada_hora = '{indice[2]}' WHERE data = '{indice[1]}' AND minuto = '{indice[2][:5]}'")
                            # print("comando_UPDATE_setup_encerrado:", comando_UPDATE_setup_encerrado)
                            cursor.execute(comando_UPDATE_setup_encerrado)
                            cursor.commit()

                            # Usa o obj_finalizar_paradas(abertas)
                            Finalizar_Paradas_Abertas(tabela_sql_producao, self.tabela_sql_resumo)

                            # Deletando o arquivo temporario:
                            comando_DELETE_setup_encerrado = (f"DELETE FROM {self.tabela_sql_resumo} WHERE temporarios LIKE '{indice[0]}•{indice[1]}•{indice[2][:5]}%'")
                            # print("comando_DELETE_setup_encerrado:", comando_DELETE_setup_encerrado)
                            cursor.execute(comando_DELETE_setup_encerrado)
                            cursor.commit()

                        except:
                            print("Deu erro de gravação!")
                            pass

        # Lista lista_producao_setup_inicio
            lista_producao_setup_inicio = [elemento for elemento in dados_recodificados if elemento[0] == 'producao_setup_inicio']
            # print("lista_producao_setup_inicio:", lista_producao_setup_inicio)
            # Se lista_setup_encerrado contiver item:
            if len(lista_producao_setup_inicio) > 0:
                for indice in lista_producao_setup_inicio:
                    # gerando data,minuto do indice:
                    data_minuto_indice = []
                    data_minuto_indice.append(indice[1])
                    data_minuto_indice.append(indice[2][:5])
                    data_minuto_indice = tuple(data_minuto_indice)
                    # print("data_minuto_indice:", data_minuto_indice)

                    # Se data,minuto do indice contiver nos 960
                    if str(data_minuto_indice) in str(valores_data_minuto_ultimos_960):
                        try:
                            # Trocar INSERT por UPDATE:
                            comando_UPDATE_producao_setup_inicio = (f"UPDATE {self.tabela_sql_resumo} SET producao = '{indice[3]}' WHERE data = '{indice[1]}' AND minuto = '{indice[2]}'")
                            # print("comando_UPDATE_producao_setup_inicio:", comando_UPDATE_producao_setup_inicio)
                            cursor.execute(comando_UPDATE_producao_setup_inicio)
                            cursor.commit()

                            # Deletando o arquivo temporario:
                            comando_DELETE_comando_UPDATE_producao_setup_inicio = (f"DELETE FROM {self.tabela_sql_resumo} WHERE temporarios LIKE '{indice[0]}•{indice[1]}•{indice[2]}%'")
                            # print("comando_DELETE_comando_UPDATE_producao_setup_inicio:", comando_DELETE_comando_UPDATE_producao_setup_inicio)
                            cursor.execute(comando_DELETE_comando_UPDATE_producao_setup_inicio)
                            cursor.commit()

                        except:
                            print("Deu erro de gravação!")
                            pass
                        
        # Lista lista_producao_setup_fim
            lista_producao_setup_fim = [elemento for elemento in dados_recodificados if elemento[0] == 'producao_setup_fim']
            # print("lista_producao_setup_fim:", lista_producao_setup_fim)
            # Se lista_setup_encerrado contiver item:
            if len(lista_producao_setup_fim) > 0:
                for indice in lista_producao_setup_fim:
                    # gerando data,minuto do indice:
                    data_minuto_indice = []
                    data_minuto_indice.append(indice[1])
                    data_minuto_indice.append(indice[2][:5])
                    data_minuto_indice = tuple(data_minuto_indice)
                    # print("data_minuto_indice:", data_minuto_indice)

                    # Se data,minuto do indice contiver nos 960
                    if str(data_minuto_indice) in str(valores_data_minuto_ultimos_960):
                        try:
                            # Trocar INSERT por UPDATE:
                            comando_UPDATE_producao_setup_fim = (f"UPDATE {self.tabela_sql_resumo} SET producao = '{indice[3]}' WHERE data = '{indice[1]}' AND minuto = '{indice[2]}'")
                            # print("comando_UPDATE_producao_setup_fim:", comando_UPDATE_producao_setup_fim)
                            cursor.execute(comando_UPDATE_producao_setup_fim)
                            cursor.commit()

                            # Deletando o arquivo temporario:
                            comando_DELETE_comando_UPDATE_producao_setup_fim = (f"DELETE FROM {self.tabela_sql_resumo} WHERE temporarios LIKE '{indice[0]}•{indice[1]}•{indice[2]}%'")
                            # print("comando_DELETE_comando_UPDATE_producao_setup_fim:", comando_DELETE_comando_UPDATE_producao_setup_fim)
                            cursor.execute(comando_DELETE_comando_UPDATE_producao_setup_fim)
                            cursor.commit()

                        except:
                            print("Deu erro de gravação!")
                            pass

        # cursor.commit()
        cursor.close()
        conexao.close()


# Exemplo de uso ===========================================================================
if __name__ == "__main__":
    Redirecionar_Temporarios("luxor_producao", "luxor_resumo")