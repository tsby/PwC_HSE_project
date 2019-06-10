# General KPI
# Данная функция - копия O_2(df) с различием в возвращаемых значениях, я использую ее вывод в других функциях,
# чтобы не портить O_2(df)
# eq_chains - события одного маршрута
# num_cases - CaseID цепочек, которые входят в маршрут
# d - словарь, в котором ключ - номер маршрута, значение - количество цеопчек этого маршрута 
# k - сколько всего маршрутов на текущий момент
# i - число для проверки того, прошли ли мы все значения словря за итерацию

def unique_paths(df):
    #Считает количество маршрутов.
    #Вход:  dataframe - eventlog, отсортированный по временной отметке и событиям. 
    #Выход: количество маршрутов.
    
    #Предобработка данных
    cases = df['CaseID'].unique()
    chain = df[df['CaseID'] == cases[0]]
    activ = chain['Activity'].tolist()
    
    #Подготовим необходимые массивы
    eq_chains = []
    num_cases = [[] for i in range(len(cases))]
    
    #Выполним первую инициализацию
    eq_chains.append(activ)
    num_cases[0].append(cases[0])
    d = {}
    d[0] = 1
    k = 1
    
    #Запустим цикл, выполняя то же самое
    for case in cases[1:]:
        #Пердобработка данных.
        chain = df[df['CaseID'] == case]
        activ = chain['Activity'].tolist()
        i = 0
        
        #Проверка на наличие такого пути
        for key in d.keys():
            if eq_chains[key] == activ:
                d[key] += 1
                num_cases[key].append(case)
                break
            i += 1
        if i == k:
            d[k] = 1
            eq_chains.append(activ)
            num_cases[k].append(case)
            k += 1
    return d, eq_chains, num_cases
	


def O_1(df):
    #Вычисляет общее количество экземпляров процесса .
    #Вход:  dataframe - eventlog, отсортированный по временной отметке и событиям.
    #Выход: общее количество цепочек.
    
    #Подсчет количества экземпляров процесса
    return len(df['CaseID'].unique())
	
	
	
def O_2(df):
    #Считает количество маршрутов.
    #Вход:  dataframe - eventlog, отсортированный по временной отметке и событиям. 
    #Выход: количество маршрутов.
    
    #Предобработка данных
    cases = df['CaseID'].unique()
    chain = df[df['CaseID'] == cases[0]]
    activ = chain['Activity'].tolist()
    
    #Подготовим необходимые массивы
    eq_chains = []
    num_cases = [[] for i in range(len(cases))]
    
    #Выполним первую инициализацию
    eq_chains.append(activ)
    num_cases[0].append(cases[0])
    d = {}
    d[0] = 1
    k = 1
    
    #Запустим цикл по оставшимся кейсам
    for case in cases[1:]:
        #Пердобработка данных.
        chain = df[df['CaseID'] == case]
        activ = chain['Activity'].tolist()
        i = 0
        
        #Проверка на наличие такого пути
        for key in d.keys():
            if eq_chains[key] == activ:
                d[key] += 1
                num_cases[key].append(case)
                break
            i += 1
            
        #Добавление нового пути
        if i == k:
            d[k] = 1
            eq_chains.append(activ)
            num_cases[k].append(case)
            k += 1
    return len(d)
	
	
def O_3(df):
    #Вход:  dataframe - eventlog.
    #Выход: количество уникальных пользователей.
    
    #Подсчет количества уникальных пользователей
    return len(df['User'].dropna().unique())
	
def O_4(df):
    #Считает число начатых и законченных экземпляров процесса (кейсов) и их долю от общего числа экземпляров процесса.
    #Вход:  dataframe - eventlog.
    #Выход: число и отношение кол-ва начатых и завершенных экземпляров процесса (кейсов)
    #       (т.е. таких экземпляров процесса (кейсов), которые закончились Катагорией События
    #       ('Activity Category') == 'Платеж (выравнивание)') к общему кол-ву экземпляров процесса.
    
    #Поиск кейсов с 'Activity Category' равным 'Заказ на поставку создан' и 'Платеж (выравнивание)'
    started_cases = set(df.iloc[np.where(df['Activity Category'] == 'Заказ на поставку создан')]['CaseID'])
    finished_cases = set(df.iloc[np.where(df['Activity Category'] == 'Платеж (выравнивание)')]['CaseID'])
    
    #Подсчет мощности пересечения найденных множеств и доли среди всех кейсов
    return len(started_cases & finished_cases), len(started_cases & finished_cases) / len(df['CaseID'].unique())
	

def O_5(df):
    # Вычисляет нестандартные экземпляры процесса (кейсы) по времени.
    # Нестандартная экземпляр процесса - экземпляр процесса, который отклоняется по времени от среднего
    # более чем на два стандартных отклонения.
    # Вход:  dataframe-eventlog.
    # Выход: кортеж(tuple). 
    #        Первое значение - количество нестандартных по времени экземпляров процесса.
    #        Второе значение - процентное соотношение этих экземпляров процесса ко всем экземплярам процесса.
    
    #Сбор всех цепочек
    cases = df['CaseID'].unique()
    
    #Рассчет времени прохождения экземпляры процесса
    all_time = df.groupby(
        ['CaseID'])['Event end'].apply(
        lambda a: (a.max() - a.min()).total_seconds()
    )
    
    #Учет в рамках маршрутов
    d, eq_chains, num_cases = unique_paths(df)
    ind = 0
    anomal_cases = []
    
    #Подсчет необходимых метрик в рамках маршрута
    for i in range(len(num_cases)): 
        c = num_cases[i]
        av = np.mean(all_time[c])
        std = np.std(all_time[c])
        to_remember = np.where(all_time[c] > av + 2 * std)[0]
        if len(to_remember) != 0:
            anomal_cases.append(num_cases[i])
        ind += len(to_remember)
    return ind, ind / len(cases)
	
	
def convert_to_adj_list(activ):
    #Переводит из последовательности событий в список смежности.(более удобное представление графа).
    #Вход:  activ - list, массив строк - последовательность событий одного экземпляра процесса.
    #Выход: res - list, массив смежности графа.
    
    #Подготовка массивов
    decode = list(set(activ))
    dec_dict = {}
    res = [[] for i in range(len(decode))]

    #Подготовка значений
    for i in range(len(decode)):
        dec_dict[decode[i]] = i

    #Перевод значений из из цепочки в граф
    for i in range(len(activ) - 1):
        res[dec_dict[activ[i]]].append(dec_dict[activ[i + 1]])

    return res

def O_6(df):
    #Считает количество сложных экземплярлв процесса. Сложный экземпляр процесса - такой экземпляра процесса, 
    #который отклоняется от среднего больше, чем на одно стандартное отклонение,
    #по количеству событий и количеству циклов. 
    #Вход:  dataframe-eventlog.
    #Выход: количество сложных экземпляров процесса.
    
    #Подготовка значений
    num_of_loop_in_case = {}
    cases = df['CaseID'].unique()
    begin = time.time()

    #Группировка по кейсам
    num_of_act_in_case = df.groupby('CaseID').agg({'Activity': 'count'})['Activity'].to_dict()

    #Запуск цикла по кейсам
    for case in cases:
        
        #Сбор кейса по активити
        chain = df.iloc[np.where(df['CaseID'] == case)]
        activ = chain['Activity'].tolist()
        
        #Создание вспомогательной переменной
        l_am = 0
        if len(activ) != len(set(activ)):

            #Вызов функции перевода в граф смежности
            conv_activ = convert_to_adj_list(activ)
            
            #Создание вспомогательного массива
            visited = [0 for i in range(len(conv_activ))]
        
            def dfs(vert, l_am):
                #Обход в глубину.
                #Вход:  массив вершин и количество циклов
                #Выход: количество циклов
                visited[vert] = 1
                for v in conv_activ[vert]:
                    if not visited[v]:
                        l_am = dfs(v, l_am)
                    else:
                        l_am += 1
                return l_am
            
            #Подсчет количества циклов для кейса
            for i in range(len(conv_activ)):
                if not visited[i]:
                    l_am += dfs(i, l_am)
        num_of_loop_in_case[case] = l_am
    
    #Подготовка необходимых массивов
    loop_in_case = np.array(list(num_of_loop_in_case.values()))
    act_in_case = np.array(list(num_of_act_in_case.values()))
    
    #Подсчет необходимых метрик
    av_loop = np.mean(loop_in_case)
    std_loop = np.std(loop_in_case) 
    ind_loop = np.where(loop_in_case > av_loop + std_loop)
    av_act = np.mean(act_in_case)
    std_act = np.std(act_in_case)
    ind_act = np.where(act_in_case > av_act + std_act)
    return len(set(ind_act[0]) & set(ind_loop[0]))
	
def O_7(df):    
    #Вычисляет среднее количество циклов на один экземпляр процесса.
    #Вход:  dataframe - eventlog.
    #Выход: среднее количество циклов на один экземпляр процесса.
    
    #Подготовка значений
    n_df = df.drop(df[df['Activity'] == 'Платеж (выравнивание)'].index)
    cases = n_df['CaseID'].unique()    
    loops = []
    ind = {}
    i = 0
    
    #Запуск цикла по всем кейсам
    for case in cases:
        
        #Подготовка значений
        chain = n_df[n_df['CaseID'] == case]
        activ = chain['Activity'].tolist()
        ind[case] = i
        events = set()
        loop_am = 0
        
        #Проверка наличия цикла
        for act in activ:
            if act in events:
                loop_am += 1
            events.add(act)
        i += 1        
        loops.append(loop_am)
        
    #Подсчет по каждому маршруту
    d, eq_chains, num_cases = unique_paths(n_df)
    av = []
    loops = np.array(loops)
    for c in num_cases: 
        summ = 0
        count = 0
        for j in c:
            summ += ind[j]
            count += 1
        if count:
            av.append(summ / count)
        else:
            av.append(0)
    return np.array(av).mean()
	
def O_8(df):
    #Вычисляет среднее количество экземпляров процесса, созданных за день.
    #(т.е. таких, у которых Событие('Activity') == 'Заказ на поставку создан')
    #Вход:  dataframe - eventlog.
    #Выход: среднее количество экземпляров процесса, созданных за день.
    
    #Подготовка данных
    df_n = pd.DataFrame(df, columns=['Event end', 'CaseID', 'Activity'])
    df_n['Event end'] = df_n['Event end'].astype(str).apply(lambda x: x[:10])
    count = df_n.iloc[np.where(df_n['Activity'] == 'Заказ на поставку создан')]['Event end']
    
    #Подсчет количества кейсов по дням
    am = sum(Counter(df_n.iloc[np.where(df_n['Activity'] == 'Заказ на поставку создан')]['Event end']).values())
    
    #Подсчет общего числа дней
    days = len(df_n['Event end'].unique())
    
    #Вывод гистограммы
    plt.figure(figsize=(25, 10))
    plt.title('Распределние создания экземпляров процесса по неделям')
    plt.xlabel('Дата')
    plt.ylabel('Количество созданий')
    plt.hist(pd.to_datetime(count.values), bins=52, alpha=0.78)
    plt.show()
    
    #Полсчет среднего числа
    return am / days
	
	
def O_9(df):
    #Вычисляет популярные категории событий относительно количества экземпляров процесса и пользователей.
    
    #Вход:  dataframe - eventlog.
    #Выход: Кортеж (act_am_dict, users_am_dict) 
    #        act_am_dict - словарь, в котором ключ - название категории событий('Activity Category'),
    #        а значение - отношение количества событий, входящих в эту категорию, и количества всех событий.
    #        users_am_dict - словарь, в котором ключ - название категории событий('Activity Category'),
    #        а значение - количество уникальных пользователей, которые участвовали в этой категории событий.
    
    #Подготовка значений
    df_n = df.drop(df[df['Activity Category'] == 'Платеж (выравнивание)'].index)
    act_am_dict = df_n.groupby('Activity Category')['CaseID'].unique().apply(lambda x: len(x))\
                  .div(len(df_n['CaseID'].unique())).to_dict()
    users_am_dict = df_n.groupby('Activity Category').agg({"User": lambda x: x.nunique()}).to_dict()['User']
    
    #Сортировка
    sorted_act_am_dict = sorted(act_am_dict.items(), key=lambda kv: kv[1], reverse=True)
    sorted_users_am_dict = sorted(users_am_dict.items(), key=lambda kv: kv[1], reverse=True)

    return sorted_act_am_dict[:5], sorted_users_am_dict[:5]
	
	
def convert_time(time):
    #Функция перевода времени
    #Вход:  время числом в секундах
    #Вывод: Сформированная строка по дням, часам, минутам и секундам
    day = time // (24 * 3600)
    time = time % (24 * 3600)
    hour = time // 3600
    time %= 3600
    minutes = time // 60
    time %= 60
    seconds = time
    
    return "%d Days %d Hours %d Minutes %d Seconds" % (day, hour, minutes, seconds)

def O_10(df):
    #Вычисляет отношение общего времени, проведенного в рамках категории события, к количеству экземпляров процесса,
    #которые были охвачены этой категорией события.
    #Вход:  dataframe - eventlog,  отсортированный по временной отметке и событиям.
    #Выход: all_time - словарь, где ключ - название категории события, а значение - среднее количество проведенного 
    #       времени в рамках соотв. категории события на экземпляр события.
    
    #Подготовка необходимых значений
    df.sort_values(by='Event end', inplace=True)
    all_time = {cat : 0 for cat in df['Activity Class'].unique()}
    case_am = {cat : 0 for cat in df['Activity Class'].unique()}
    cases = df['CaseID'].unique()
    
    #Запускаем цикл по всем кейсам
    for case in cases:
        
        #Подготовка необходимых значений
        chain = df.iloc[np.where(df['CaseID'] == case)]
        cat = chain['Activity Class'].tolist()
        time = chain['Event end'].tolist()
        
        #Подсчет времени для каждого класса кейса
        for i in range(len(cat) - 2):
            all_time[cat[i]] += (time[i + 1] - time[i]).total_seconds()
        
        #Подсчет количества кейсов, у которых были классы
        for cl in cat:
            case_am[cl] += 1
    return case_am
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
