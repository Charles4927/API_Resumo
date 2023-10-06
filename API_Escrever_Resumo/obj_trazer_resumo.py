from datetime import datetime, timedelta
from obj_conexoes_bco_dados import Conexoes_SQL


# O objetivo desta classe é montar um resumo de produção e paradas
class DadosResumidos:
    # 1 - inicia a função com o parâmetro especificado
    def __init__(self, data_e_horario, tabela_sql_producao):
        self.data_e_horario = str(data_e_horario)
        self.tabela_sql_producao = str(tabela_sql_producao)

    def producao_ultimos_60seg(self):
        tempo_atual_str = self.data_e_horario  # Aqui está em str
        # tempo_atual_str = str('2023-05-01 00:07:00')  # Aqui está em str
        tempo_atual_date = datetime.strptime(tempo_atual_str, "%Y-%m-%d %H:%M:%S")  # Aqui está em datetime
        tempo_atras_date0 = datetime.strptime(tempo_atual_str, "%Y-%m-%d %H:%M:%S")
        tempo_atras_date = tempo_atras_date0 - timedelta(minutes=1, seconds=30)  # Ajuste aqui qtd tempo atras
        tempo_atras_str = str(tempo_atras_date)
        # print("tempo_atras_str", tempo_atras_str)

        data_atras_str = tempo_atras_date.strftime("%d/%m/%Y")
        data_atual_str = tempo_atual_date.strftime("%d/%m/%Y")

        tempo_atual_portug_str = datetime.strftime(tempo_atual_date, "%d/%m/%Y %H:%M:%S")
        tempo_atras_portug_str = datetime.strftime(tempo_atras_date, "%d/%m/%Y %H:%M:%S")

        # Abrindo consulta no banco de dados:
        conexao = Conexoes_SQL('cdtmes').obter_conexao()
        cursor = conexao.cursor()

        # Montando o comando Sql para efetuar a consulta
        comm_sql1 = ""
        if data_atras_str == data_atual_str:
            comm_sql1 = ("SELECT Data, Hora FROM " + self.tabela_sql_producao + " where Data = '" + data_atual_str + "'")
        else:
            comm_sql1 = ("SELECT Data, Hora FROM " + self.tabela_sql_producao + " where Data = '" + data_atras_str + " or " + data_atual_str + "'")
        # print("comm_sql1", comm_sql1)

        cursor.execute(comm_sql1)
        valores = cursor.fetchall()
        # print("valores:", valores)

        cursor.close()
        conexao.close()

        # print("valores:", valores)

        # Todos os horarios do intervalo
        inicio = [tempo_atras_portug_str[:10], tempo_atras_portug_str[10:]]
        fim = [tempo_atual_portug_str[:10], tempo_atual_portug_str[10:]]

        # Criando a lista ini_fim
        lista = [inicio, fim]
        lista_datas = []
        data_inicial = tempo_atras_date
        data_final = tempo_atual_date
        lista_datas.append([data_inicial.strftime('%d/%m/%Y'), data_inicial.strftime('%H:%M:%S')])
        while data_inicial < data_final:
            data_inicial += timedelta(seconds=1)
            lista_datas.append([data_inicial.strftime('%d/%m/%Y'), data_inicial.strftime('%H:%M:%S')])
        self.ini_fim = list(lista_datas)
        self.ini_fim_com_30seg = self.ini_fim[-60:]
        # print("self.ini_fim:", self.ini_fim)
        # print("len self.ini_fim:", len(self.ini_fim))
        # print("self.ini_fim_com_30seg:", self.ini_fim_com_30seg)
        # print("len self.ini_fim_com_30seg:", len(self.ini_fim_com_30seg))

        ultimos_90_ciclos = valores[:]  # [-2000:] Ajuste aqui qtde ultimos registros
        ultimos_90_ciclos = [list(tupla) for tupla in ultimos_90_ciclos]
        # print("ultimos_90_ciclos:", ultimos_90_ciclos)
        # print("qtd ultimos_90_ciclos:", len(ultimos_90_ciclos))

        com_ciclos = [item for item in self.ini_fim if item in ultimos_90_ciclos]
        # print("com_ciclos:", com_ciclos)
        # print("qtd com_ciclos:", len(com_ciclos))

        sem_ciclos = [item for item in self.ini_fim if item not in ultimos_90_ciclos]
        # print("sem_ciclos:", sem_ciclos)
        # print("qtd sem_ciclos:", len(sem_ciclos))

        val_s_ciclos = []
        for item in self.ini_fim:
            if item in sem_ciclos:
                val_s_ciclos.append(1)
            else:
                val_s_ciclos.append(0)
        # print("val_s_ciclos:", val_s_ciclos)

        self.val_c_ciclos = []
        for item in self.ini_fim:
            if item in com_ciclos:
                self.val_c_ciclos.append(1)
            else:
                self.val_c_ciclos.append(0)
                self.val_c_ciclos_60seg = self.val_c_ciclos[31:] # Retirando 31 do começo da lista
        # print("val_c_ciclos:", self.val_c_ciclos)
        # print("val_c_ciclos_60seg:", self.val_c_ciclos_60seg)
        # print("len val_c_ciclos_60seg:", len(self.val_c_ciclos_60seg))

        producao_ultimos_60seg = self.val_c_ciclos[-60:].count(1)

        if producao_ultimos_60seg == 0:
            producao_ultimos_60seg = ""

        return producao_ultimos_60seg
        # print("producao_60seg:", producao_60seg)

    def lista_paradas(self):
        seg_pos = 30
        seg_pre = 0
        # Adicionar segundos de tolerancia de produção
        prod_com_ciclos = self.val_c_ciclos.copy()
        for i, item in enumerate(self.val_c_ciclos):
            if item == 1:
                for j in range(max(0, i - seg_pre), min(len(prod_com_ciclos), i + seg_pos)):
                    prod_com_ciclos[j] = 1
        # print("prod_com_ciclos:", prod_com_ciclos)
        # print("len prod_com_ciclos:", len(prod_com_ciclos))

        seg_a_seg_60seg = prod_com_ciclos[-60:]
        # print("seg_a_seg_60seg:", seg_a_seg_60seg)
        # print("seg_a_seg_60seg:", len(seg_a_seg_60seg))

        # Separar qtde de caracteres por evento
        contagem_60_seg_a_seg = []
        numero_anterior = seg_a_seg_60seg[0]
        contador = 0
        for n in seg_a_seg_60seg:
            if n == numero_anterior:
                contador += 1
            else:
                contagem_60_seg_a_seg.append(contador)
                contador = 1
                numero_anterior = n
        contagem_60_seg_a_seg.append(contador)
        # print("contagem_60_seg_a_seg:", contagem_60_seg_a_seg)

        ########################################################################
# Começa
        # cod Bin # Separar inicios e fins de paradas em duas listas:
        lista1 = seg_a_seg_60seg
        # indices com valor "0" após o "1"
        inicios_p = [i for i in range(len(lista1)) if lista1[i - 1] == 1 and lista1[i] == 0]
        # print("inicios_p:", inicios_p)

        # indices com valor "0" antes de "1"
        fins_p = [i for i in range(len(lista1)) if lista1[i - 1] == 0 and lista1[i] == 1]
        # print("fins_p:", fins_p)

        # # Para corrigir o problema quando a parada está em aberto:
        # if inicios_p[:-1] > fins_p[:-1]:
        #     inicios_p = [0] + inicios_p
        #     fins_p = fins_p + [len(ini_fim)-1]
        #     print("inicios_p2", inicios_p)

        # cod Bin # Concatenar horarios de inicios de paradas com fins de paradas
        inp = inicios_p
        fip = fins_p
        lista = []
        for inp, fip in zip(inp, fip):
            lista.append([inp, fip])

        # Com base no cod bin que começou a parada, estamos trazendo o horario exato, só inicios de paradas
        inis_paradas = [self.ini_fim_com_30seg[i] for i in inicios_p]
        # print("inis_paradas:", inis_paradas)

        # Com base no cod bin que acabou a parada, estamos trazendo o horario exato, só fins de paradas
        fins_paradas = [self.ini_fim_com_30seg[i] for i in fins_p]
        # fins_paradas.pop(0)
        # print("fins_paradas:", fins_paradas)

        # Retirando repetidos na lista começo de parada
        lista_final1 = []
        for i in inicios_p:
            if i not in fins_p:
                lista_final1.append(i)
        intervalo_hr_i = [self.ini_fim_com_30seg[i] for i in lista_final1] # Talvez mude aqui

        # Retirando repetidos na lista fim de parada
        lista_final2 = []
        for i in fins_p:
            if i not in inicios_p:
                lista_final2.append(i)
        intervalo_hr_f = [self.ini_fim_com_30seg[i] for i in lista_final2] # Talvez mude aqui

        # Concatenar horarios exatos de inicios de paradas com fins de paradas
        inp = intervalo_hr_i
        fip = intervalo_hr_f
        intervalo = []
        for inp, fip in zip(inp, fip):
            intervalo.append([inp, fip])

        if len(intervalo) > 1:
            intervalo.pop(-0)

        # print("intervalo:", intervalo)
        # print("tamanho intervalo:", len(intervalo))

        if len(intervalo) > 0:
            par_ini = intervalo[0][0]
            par_fim0 = intervalo[0][-1:]
            par_fim = par_fim0[0]
            # print("par_ini:", par_ini)
            # print("par_fim:", par_fim)

            parada_encontrada = []
            if par_ini > par_fim:
                parada_encontrada.append([par_ini, [""]])
            else:
                parada_encontrada.append([par_ini, par_fim])
        else:
            parada_encontrada = []
        # print("parada_encontrada:", parada_encontrada)

        # Vou colocar aqui
        se_f_2 = [[['', ''], ['', '']]]
        se_f_0 = ['', '', '', '']

        # print("len parada_encontrada:", len(parada_encontrada))
        # print("parada_encontrada:", parada_encontrada)

        parada_inserir_no_bco = []
        try:
            if len(parada_encontrada) == 1:
                parada_inserir_no_bco.append(parada_encontrada[0][0][0])  # list, dia, hora
                parada_inserir_no_bco.append(parada_encontrada[0][0][1])
                parada_inserir_no_bco.append(parada_encontrada[0][1][0])
                parada_inserir_no_bco.append(parada_encontrada[0][1][1])
            elif len(parada_encontrada) == 2:
                elf_1 = (parada_encontrada[:][0][:])
                parada_inserir_no_bco.extend(elf_1)  # list, dia, hora
                parada_inserir_no_bco.append(se_f_2[0][1][0])
                parada_inserir_no_bco.append(se_f_2[0][1][1])
            elif len(parada_encontrada) == 0:
                elf_1 = (parada_encontrada[:][0][:])
                parada_inserir_no_bco.extend(elf_1)
        except:
            parada_inserir_no_bco.extend(se_f_0)

        # print("tmanho parada_inserir_no_bco:", len(parada_inserir_no_bco))

        if len(parada_inserir_no_bco) > 4:
            parada_inserir_no_bco.pop(-1)
            parada_inserir_no_bco.pop(-1)

        if len(parada_inserir_no_bco) > 4:
            parada_inserir_no_bco.pop(-1)

        # print("parada_inserir_no_bco:", parada_inserir_no_bco)

        if parada_inserir_no_bco.count("") == 2:
            parada_inserir_no_bco.pop(-1)
            parada_inserir_no_bco.pop(-1)
            parada_inserir_no_bco.append("aberta")
            parada_inserir_no_bco.append("aberta")

        return parada_inserir_no_bco



############ Usando a classe #################

# argumentos = DadosResumidos('2023-06-20 01:08:00', 'lam04_producao')
#
# pecas = (argumentos.producao_ultimos_60seg())
# lista = (argumentos.lista_paradas())
#
# print("pecas:", pecas)
# print("lista_paradas:", lista)