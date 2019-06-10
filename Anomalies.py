# Anomalies

# Функция, вычисляющая количество нарушений одним пользователем разграничения обязанностей в кейсе
def find_quantity_of_violations(incomp_events, activ):
    # Вход:  1)  Массив несовместимых событий
    #        2) События одного кейса в упорядоченном порядке 
    # Выход: Количество нарушений пользователя в одном кейсе
    
    num_of_violations = 0 # Количество нарушений в кейсе
    
    # Для каждого события подсчитываем  его количество повторений в кейсе
    sorted_col = Counter(activ)
    
    # Увеличение счетчика num_of_violations на число, равное количеству выполнений пользователем несовместимого события 
        num_of_violations += sorted_col[incomp_events[i]]
    return num_of_violations

# Функция, вычисляющая для каждого пользователя количество нарушений по всем кейсам и общее количество нарушений разграничения обязанностей в журнале событий
def SOD_anomalies(df):
    # Вход:  Журнал событий
    # Выход: Список из двух элементов. В первом элементе хранится список пользователей, отсортированный по количеству нарушений, а во втором общее количество нарушений
 
    # Запоминаем название события в incomp_event и соответствующие ему несовместимые события в incomp_with
    incomp_event1 = 'Заказ на поставку создан'
    incomp_event2 = 'Создание Заявки'
    incomp_event3 = 'Требование авансового платежа-Возврат'
    incomp_event4 = 'Счет заведен'
    incomp_with1 = ['Заказ на поставку согласован 1', 'Заказ на поставку согласован 2', 'Платеж (выравнивание)', 
                    'Заказ на поставку блокирован', 
                    'Заказ на поставку: изменен статус выпуска: ДанныеОтпр, возможны изменения',
                    'Заказ на поставку: изменен статус выпуска: ДанныеСогл, измен не возможны', 
                    'Заказ на поставку: изменен статус выпуска: НачКод, возможны изменения', 
                    'Заказ на поставку: изменен статус выпуска: Согл, измен не возможны']
    incomp_with2 = ['Заявка согласована']
    incomp_with3 = ['Авансовый платеж', 'Перерасчет авансового платежа', 'Перерасчет авансового платежа-Возврат', 
                    'Перерасчет по требованию авансового платежа']
    incomp_with4 = ['Счет блокирован: несоответствие цены', 'Счет предварительно полностью зарегистрирован', 
                    'Счет блокирован: несоответствие даты', 
                    'Счет предварительно зарегистрирован']
    
    # Группируем  колонку событий из входных данных  по  индентификатору  кейса и пользователю
    arrcase = df.groupby(['CaseID', 'User'])['Activity'].apply(list)
    arrcase = arrcase.reset_index()
    
    # Вспомогательные списки и переменные 
    users_violations_track = [] # Хранит списки из двух элементов. Первым элементом является пользователь, а вторым - количество его нарушений
    users = arrcase['User'].unique()
    users = list(users) # Список пользователей
    violations = 0
    total_violations = 0
    
    # Заполнение массива users_violations_track
    for i in range(len(users)):
        users_violations_track.append([users[i], 0])
        
    # Ищем нарушения разграничений обязанностей
    for i in range(len(arrcase['User'])):
        # Находим индекс пользователя в  списке пользователей
        user_index = users.index(arrcase['User'][i])
        
        # Проверяем выполнял ли пользователь событие, у которого есть список несовместимых событий
        if(incomp_event1 in arrcase['Activity'][i]):
            
            # Находим количество нарушений в текущем кейсе при помощи функции find_quantity_of_violations
            violations = find_quantity_of_violations(incomp_with1, arrcase['Activity'][i])
    
            #Обновление счетчиков
            users_violations_track[user_index][1] += violations # Увеличиваем счётчик нарушений для текущего пользователя
            total_violations += violations # Увеличиваем счетчик для общего количества нарушений
            
        # Далее, рассматриваем все события, у которых есть список несовместимых событий, аналогично трем предыдущим этапам
        if(incomp_event2 in arrcase['Activity'][i]):
            violations = find_quantity_of_violations(incomp_with2, arrcase['Activity'][i])
            users_violations_track[user_index][1] += violations
            total_violations += violations
        if(incomp_event3 in arrcase['Activity'][i]):
            violations = find_quantity_of_violations(incomp_with3, arrcase['Activity'][i])
            users_violations_track[user_index][1] += violations
            total_violations += violations 
        if(incomp_event4 in arrcase['Activity'][i]):
            violations = find_quantity_of_violations(incomp_with4, arrcase['Activity'][i])
            users_violations_track[user_index][1] += violations
            total_violations += violations
            
    # Сортируем пользовтелей в порядке убывания количества нарушений
    users_violations_track = sorted(users_violations_track, key = lambda x: x[1], reverse = True)
    
    # Отсекаем пользователей, у которых нету нарушений
    users_violations_track = list(filter(lambda x: x[1] != 0, users_violations_track))
    
    # Возвращаем список, первым элементом которого является отсортированный список пользователей нарушивших
    # разграничение обязанностей, а вторым  общее количество нарушений разграничения обязанностей
    return [users_violations_track, total_violations]
	
	
	
def eff_anomaly_detection(df):
    cases = df['CaseID'].unique()
    classes = df['Activity Class'].unique()
    features = np.zeros((len(cases), len(classes)))

    for i in range(len(cases)):
        chain = df[df['CaseID'] == cases[i]]    
        chain_count = chain.groupby('Activity Class')['Activity'].value_counts()
        for j in range(len(classes)):
            if classes[j] in chain_count:
                features[i][j] = chain_count[classes[j]].sum()
            
    mean_values = np.zeros(len(classes))

    tran_feat = features.T

    for i in range(len(classes)):
        mean_values[i] = tran_feat[i].mean()
    
    fin_dict = dict(zip(classes, mean_values))

    anomaly = np.zeros((len(cases), len(classes)))

    for j in range(len(classes)):
        anomaly[np.where(tran_feat[j] > mean_values[j] + 2 * tran_feat[j].std())[0]] = 1
    
    amount = len(np.unique(np.where(anomaly == 1)[0]))
    
    return amount, amount / len(cases)
	
	
#Выявления цепочек с аномальным временем выполнения
def duration_anomaly(df):
    #Подготовка данных к обработке
    df = pd.read_csv('Event_log.txt', sep='\t', encoding='cp1251')
    df['Event end'] = pd.to_datetime(df['Event end'])
    df = df.sort_values(by='Event end')

    #Обработка данных: отбор закрытых кейсов
    started_cases = set(df.iloc[np.where(df['Activity Category'] == 'Заказ на поставку создан')]['CaseID'])
    finished_cases = set(df.iloc[np.where(df['Activity Category'] == 'Платеж (выравнивание)')]['CaseID'])
    finished = started_cases & finished_cases
    df['finished'] = df['CaseID'].apply(lambda x: x in finished)
    df = df[df['finished'] == 1]

    #Обработка данных: группировка событий в кейсы
    cases = df.groupby(['CaseID'])['Activity'].apply(lambda x: x.sum())
    cases = cases.reset_index()
    casesun = cases.drop_duplicates(subset=['Activity'])
    finished_cases
    casesun['Newcount']=casesun['Activity'].map(cases['Activity'].value_counts())
    arrcase = df.groupby(['CaseID'])['Activity'].apply(list)
    times = df.groupby(['CaseID'])['Event end'].apply(list)

    #Анализ данных: вычисление длительности каждого события
    def event_dur(arr):
        return np.array(arr[1:]) -  np.array(arr[:-1])
    durs = times.apply(lambda x: event_dur(x))
    for i in durs.index:
        for j in range(len(durs[i])):
            durs[i][j] = durs[i][j].total_seconds()
    case_durs = durs.reset_index()

    #Анализ данных: вычисление средней длительности события для каждого кейса
    case_durs['average'] = case_durs['Event end'].apply(lambda x: np.mean(x))

    #Анализ данных: вычисление средней длительности события для всех кейсов
    aver = np.mean(case_durs['average'])
    standard = np.std(case_durs['average'])

    #Идентификация аномальный кейсов
    case_durs['anomaly'] = case_durs['average']  > (aver + standard)



    #Вывод результата
    def ConvertSectoDay(n): 
  
        day = n // (24 * 3600) 
  
        n = n % (24 * 3600) 
        hour = n // 3600
  
        n %= 3600
        minutes = n // 60
  
        n %= 60
        seconds = n 
      
        return int(day), int(hour), int(minutes), int(seconds)
    plt.hist(case_durs['average'][case_durs['anomaly'] == True], bins=20, alpha=0.78, range=[1, max(case_durs['average'])])
    plt.grid()
    plt.ylabel('Number of cases')
    plt.xlabel('Average time of activity in the process path')
    #print('Количество кейсов, в которых выявлены аномалии: ' + str(sum(case_durs['average']  > (aver + standard))))
    #res = ConvertSectoDay(aver + standard)
    #print('Средняя  длительность события для всех кейсов: ' + str(res[0]) + ' дней ' + str(res[1]) + ' часов ' + str(res[2]) + ' минут ' + str(res[3]) + ' секунд')
    #print('Отношение кейсов, в которых выявлены аномалии, к общему числу всех кейсов: ' + str(int(100 * round(sum(case_durs['average']  > (aver + standard))/len(cases), 2))) + '%')
    plt.savefig('gist.png')
    
    return sum(case_durs['average']  > (aver + standard)), int(100 * round(sum(case_durs['average']  > (aver + standard))/len(cases), 2))
