# P2P KPI

def drop_unfinished(df):
    # Убирает незакрытые цепочки
    # Вход:  датафрейм
    # Выход: датафрейм, который содержит только закрытые цепочки
    
    # Подготавливаем два сета: первый содержит цепочки с категорией события 'Заказ на поставку создан', второй
    # содержит категорию события 'Платеж (выравнивание)'
    started_cases = set(df.iloc[np.where(df['Activity Category'] == 'Заказ на поставку создан')]['CaseID'])
    finished_cases = set(df.iloc[np.where(df['Activity Category'] == 'Платеж (выравнивание)')]['CaseID'])
    
    # Находим дополнение к пересечению этих множеств
    all_cases  = set(df['CaseID'].unique())
    ind = list(all_cases - (started_cases & finished_cases))
    
    
    # Убираем все кейсы из этого множества
    for case in ind:
        df = df.drop(df[df['CaseID'] == case].index)
    
    # Перенумерование индексов
    df= df.reset_index(drop = True)
    return df
	
	
# P2P-1: Объём закупок
def P2P_1(df):
    
    # Подготовка необходимого массива и переменной
    visited_cases = [] # Запоминает кейсы, которые мы уже посмотрели
    total_amount = 0 # Сумма всех трат на закупки
    
    # Проходим по всему по набору данных и рассматриваем все кейсы
    for i in range(len(df["CaseID"])):
        # Если кейс мы уже посетили, то переходим на следующую итерацию цикла
        if(df["CaseID"][i] in visited_cases):
            continue
            
        # Запоминаем кейс, в котором побывали
        visited_cases.append(df["CaseID"][i])
        
        # Проверка на nan и обновление суммы трат на закупки
        if(not np.isnan(df["Amount"][i])):
                total_amount += df["Amount"][i]

    return total_amount
	
	
# P2P-2:  Отношение заказов товаров к заказам услуг
def P2P_2(df):
    
    # Подготовка необходимых массивов и переменных
    cases = df['CaseID'].unique() # Составляем массив из идентификаторов кейсов
    visited_cases = [] # Запоминает кейсы, которые мы уже посмотрели
    services = 0 # Количество заказов  услуг
    goods = 0 # Количество заказов товаров
    
    # Сохраняем два типа заказов
    Purchase_type = df['Purchase type'].unique()
    
    # Проходим по всему по набору данных и рассматриваем все кейсы
    for i in range(len(df["CaseID"])):
        
        # Если кейс мы уже посетили, то переходим на следующую итерацию цикла
        if(df["CaseID"][i] in visited_cases):
            continue
            
        # Запоминаем кейс, в котором побывали
        visited_cases.append(df["CaseID"][i])
        
        # Обновляем переменные количества заказов услуг и заказов товаров соответственно
        if(df['Purchase type'][i] == Purchase_type[0]):
            services += 1
        else:
            goods += 1
    
    # Возвращаем отношение количества заказов товаров к количеству заказов услуг
    return goods/services
	
	
# P2P-3:  Отношение трат на товары к тратам на услуги
def P2P_3(df):
    
    # Подготовка необходимых массивов и переменных
    cases = df['CaseID'].unique() # Составляем массив из идентификаторов кейсов
    visited_cases = [] # Запоминает кейсы, которые мы уже посмотрели
    services_total_amount = 0 # Сумма, потраченная на услуги
    goods_total_amount = 0 # Сумма, потраченная на товары
    
    # Сохраняем два типа заказов
    Purchase_type = df['Purchase type'].unique()
    
    # Проходим по всему по набору данных и рассматриваем все кейсы
    for i in range(len(df["CaseID"])):
        
        # Если кейс мы уже посетили, то переходим на следующую итерацию цикла
        if(df["CaseID"][i] in visited_cases):
            continue
            
        # Запоминаем кейс, в котором побывали
        visited_cases.append(df["CaseID"][i])
        
        # Обновление переменных
        if(df['Purchase type'][i] == Purchase_type[0]):
            if(not np.isnan(df["Amount"][i])):
                services_total_amount += df["Amount"][i]
        else:
            if(not np.isnan(df["Amount"][i])):
                goods_total_amount += df["Amount"][i]
    
    # Возвращаем отношение суммы, потраченной на товары, к сумме, потраченной на услуги
    return (goods_total_amount/services_total_amount)
	
	
# P2P-4: Процент товаров, которые поставляются исключительно внутренними поставщиками
def P2P_4(df):
    
    # Подготовка необходимых массивов и переменных
    Material = list(df['Material'].unique()) # Запоминаем все уникальные наименования товаров
    quantity_of_mat = len(Material) # Запоминаем, сколько у нас было уникальных товаров
    total_amount_domestic_mat = 0 # Храним количество внутренних товаров
    
    # Проходим по всему набору данных и рассматриваем все кейсы
    for i in range(len(df["CaseID"])):
        
        # Проверяем встречали ли мы данный товар(если нет, то он будет в списке Material)
        if(df["Material"][i] in Material):
            
            # проверяем является ли поставщик внутренним и обновляем количество внутренних товаров
            if(df["Supplier_type"][i] == 'Внутренние'):
                total_amount_domestic_mat += 1

            # Удаляем данный товар из списка всех товаров 
            Material.pop(Material.index(df["Material"][i]))
    
    return total_amount_domestic_mat
	
	
# P2P-5: Количество неправильно завершенных частичных поставок
def P2P_5(df):
    
    # Подготовка необходимых массивов и переменных
    activ = ['Поступление материала-Возврат-Частичная поставка', 'Поступление материала-Получение-Завершающая поставка']
    quantity_of_wrong_cases = 0 # Количество неправильно завершенных частичных поставок
    cases = df['CaseID'].unique() 
    cases = list(cases) # Составляем массив из идентификаторов кейсов
    case_info = [] 
    
    # Создаём вспомогательный список
    for j in range(len(cases)):
        case_info.append([cases[j]])
    
    # Находим индекс кейса в нашем списке из идентификаторов кейсов
    for i in range(len(df["CaseID"])):
        case_index = cases.index(df['CaseID'][i])
        
        # Проверка наличия частичной поставки в текущем кейсе
        if(df["Activity"][i] == activ[0]):
            case_info[case_index].append(activ[0])
        
        # Проверка наличия завершающей поставки в текущем кейсе
        if(df["Activity"][i] == activ[1]):
            case_info[case_index].append(activ[1]) 
            
     # Проверка наличия полной поставки после частичной
    for n in range(len(case_info)):
        if(activ[0] in case_info[n] and activ[1] not in case_info[n]):
            quantity_of_wrong_cases += 1
    return quantity_of_wrong_cases
	
	
def PrintTime(secs):
    # Переводит число секунд в строку, преобразуемыую всё в удобный нам формат дд:чч:мм:сс
    # Вход:  Число секунд
    # Выход: Отформатированная строка в виде дд:чч:мм:сс
    
    # Подготовка необходимых значений 
    SECONDS_IN_DAY = 86400
    days = int(secs / SECONDS_IN_DAY) 
    sec = datetime.timedelta(seconds = (secs - days * SECONDS_IN_DAY))
    d = datetime.datetime(1,1,1) + sec
    
    # Подбор правильных окончаний для дней, часов, минут и секунд
    dayn = "days"
    hourn = "hours"
    minuten = "minutes"
    secondn = "seconds"
    if(days - 1 == 1):
        dayn = "day"
    elif(days >= 1 and days <= 4):
        dayn = "days"
    if(d.hour == 1 or d.hour == 21):
        hourn = "hour"
    elif((d.hour >= 2 and d.hour <= 4) or d.hour >= 22 and d.hour <= 24):
        hourn = "hours"
    if((d.minute % 10) == 1 and not d.minute == 11):
        minuten = "minute"
    elif((d.minute % 10) >= 2 and (d.minute % 10) <= 4 and not (d.minute >= 12 and d.minute <= 14)):
        minuten = "minutes"
    if((d.second % 10) == 1 and not d.second == 11):
        secondn = "second"
    elif((d.second % 10) >= 2 and (d.second % 10) <= 4 and not (d.second >= 12 and d.second <= 14)):
        secondn = "seconds"
    return "%s %s %s %s %s %s %s %s" % (days, dayn, d.hour, hourn, d.minute, minuten, d.second, secondn)

# P2P-6 Среднее время подтверждения заказа
def P2P_6(df):
    
    # Подготовка необходимых массивов
    cases = df['CaseID'].unique()
    cases = list(cases)
    case_time_list = []
    
    # Создание вспомогательного списка
    for i in range(len(cases)):
        case_time_list.append([cases[i], False])
    approved_times = []
    approved = ['Заказ на поставку согласован 1', 'Заказ на поставку согласован 2']
    
    # Сортировка значений с выставлением корректных значений
    df_time_sort = df.sort_values(by = 'Event end')
    df_time_sort = df_time_sort.reset_index(drop = True)
    
    # Запуск цикла с конца датафрейма
    for j in range(len(df_time_sort) - 1, -1, -1):
        
        # Проверка на согласование заказа
        case_index = cases.index(df_time_sort['CaseID'][j])
        if(df_time_sort["Activity"][j] in approved and not case_time_list[case_index][1]): 
            
            # Вычисление времени подтверждения заказа с учетом возможных ошибок
            try:
                
                # Вариант с форматом даты  "%Y-%m-%d %H:%M:%S.%f"
                approved_time = time.mktime(time.strptime(str(df_time_sort['Event end'][j]), "%Y-%m-%d %H:%M:%S.%f"))

            # При ошибке  используем формат "%Y-%m-%d %H:%M:%S"
            except ValueError:
                approved_time = time.mktime(time.strptime(str(df_time_sort['Event end'][j]), "%Y-%m-%d %H:%M:%S"))
            
            # Сохранение значения
            case_time_list[case_index].append(approved_time)
            case_time_list[case_index][1] = True
            
        # Проверка времени создания заказа  
        if(df_time_sort["Activity"][j] == 'Заказ на поставку создан'):
            
            # Вычисление времени создания заказа с учетом возможных ошибок 
            try:
                
                # Вариант с форматом даты как "%Y-%m-%d %H:%M:%S.%f"
                starting_time = time.mktime(time.strptime(str(df_time_sort['Event end'][j]), "%Y-%m-%d %H:%M:%S.%f"))
                
            # При ошибке  используем формат "%Y-%m-%d %H:%M:%S"    
            except ValueError:
                starting_time = time.mktime(time.strptime(str(df_time_sort['Event end'][j]), "%Y-%m-%d %H:%M:%S"))
            case_time_list[case_index].append(starting_time)
            
    # Подсчет времени между согласованием и созданием заявки 
    cases_without_confirm = 0
    for n in range(len(case_time_list)):
        if(len(case_time_list[n]) == 4):
            approved_times.append(case_time_list[n][2] - case_time_list[n][3])
        if(len(case_time_list[n]) == 3):
            cases_without_confirm += 1
    if(len(approved_times) == 0):
        return 0
    else:
        
        # Подсчет:
        # Среднего времени согласования заказов в формате дд:чч:мм:сс
        # Процент кейсов без согласования среди всех кейсов
        # Процент согласованных кейсов среди всех кейсов
        return [PrintTime(sum(approved_times)/len(approved_times)),
                round(cases_without_confirm/(cases_without_confirm + len(approved_times)) * 100),
                round(len(approved_times)/(cases_without_confirm + len(approved_times)) * 100)]
				
				
# P2P-7 Среднее количество изменений заказа
def P2P_7(df):
    
    # Создание необходимых массивов и значений
    cases = df['CaseID'].unique()
    quantity_of_changes = 0
    visited_cases = []
    
    # Создание списка всех соaбытий, которые считаются изменением заказа
    changes = ['Заказ на поставку бессрочно заблокирован', 'Заказ на поставку изменен: лимит на недопоставку', 
             'Заказ на поставку изменен: группа закупки', 'Заказ на поставку изменен: лимит на сверх-поставку',
             'Заказ на поставку изменен: материал', 'Заказ на поставку изменен: налоговые условия',
             'Заказ на поставку изменен: запланированный срок доставки в днях', 
              'Заказ на поставку изменен: эффективная стоимость', 'Заказ на поставку изменен: уменьшена стоимость',
             'Заказ на поставку изменен: уменьшена цена', 'Заказ на поставку изменен: уменьшено количество',
             'Заказ на поставку изменен: увеличена стоимость', 'Заказ на поставку изменен: увеличено количество',
             'Заказ на поставку изменен: увеличена цена', 'Заказ на поставку изменен: срока предоставления скидки',
             'Заказ на поставку изменен: условия оплаты', 'Заказ на поставку изменен: завод', 
              'Заказ на поставку изменен: статус наличия счета', 'Заказ на поставку изменен: валюта',
             'Заказ на поставку изменен: поставщик', 'Заказ на поставку изменен: тип документа']
    
    # Поиск изменений заказа в кейсах
    for i in range(len(df["CaseID"])):
        if(df["Activity"][i] in changes):
            quantity_of_changes += 1
            if(df["CaseID"][i] not in visited_cases):
                visited_cases.append(df["CaseID"][i])
                
    # Подсчет доли кейсов с изменениями и их число
    return [quantity_of_changes/len(cases), len(visited_cases)]
	
	
# P2P-8 Поставщики с высоким уровнем возвратов
def P2P_8(df):

    # Создание необходимых массивов
    cases = df['CaseID'].unique()
    suppliers_info = []
    suppliers = list(df["Supplier"].unique())
    
    # Запуск цикла по всем поставщикам
    for i in range(len(suppliers)):
        
        # Создание вспомогательного массива
        suppliers_info.append([suppliers[i], 0, 0, 0])
    
    # Запуск цикла по всему датафрейму
    for j in range(len(df)):
        
        # Проверка на наличие возврата у поставщика
        supplier_index = suppliers.index(df["Supplier"][j])
        if(df["Activity Category"][j] == "Поступление материала-Возврат"):
            suppliers_info[supplier_index][1] += 1 
            
        # Провекра кейса на то, что уже был рассмотрен в рамках данного поставщика
        if(df["CaseID"][j] not in suppliers_info[supplier_index]):
            suppliers_info[supplier_index][2] += 1
            suppliers_info[supplier_index].append(df["CaseID"][j])
    
    # Поиск поставщика с максимальным количеством заказов
    maximum_orders = 0
    for n in range(len(suppliers_info)):
        if(maximum_orders < suppliers_info[n][2]):
            maximum_orders = suppliers_info[n][2]
    
    # Подсчет взвешенного среднего для каждого поставщика
    for r in range(len(suppliers_info)):
        if(maximum_orders == suppliers_info[r][2]):
            suppliers_info[r][3] = suppliers_info[r][1]/((maximum_orders + 1) - suppliers_info[r][2])
            continue
        suppliers_info[r][3] = suppliers_info[r][1]/(maximum_orders - suppliers_info[r][2])

    # Сортировка по взвешенному среднему
    suppliers_info.sort(key = lambda x: x[3], reverse = True)
    quantity_of_danger_sup = 0
    danger_suppliers = []
    mean_weight = suppliers_info[0][3]/2
    
    # Подсчет числа постващиков с высоким уровенем возвратов
    for g in range(len(suppliers_info)):
        if(suppliers_info[g][3] > mean_weight):
            quantity_of_danger_sup += 1
            danger_suppliers.append(suppliers_info[g][0])
        else:
            break
    return quantity_of_danger_sup
	
	
# P2P-9 Процент заказов у поставщиков с высоким уровнем возвратов
def P2P_9(df):
    
    # Создание необходимых массивов
    cases = df['CaseID'].unique()
    suppliers_info = []
    suppliers = list(df["Supplier"].unique())
    
    # Создание вспомогательного массива
    for i in range(len(suppliers)):
        suppliers_info.append([suppliers[i], 0, 0, 0])
    
    # Запуск цикла по всему датафрейму
    for j in range(len(df)):
        
        # Проверка на наличие возврата у поставщика
        supplier_index = suppliers.index(df["Supplier"][j])
        if(df["Activity Category"][j] == "Поступление материала-Возврат"):
            suppliers_info[supplier_index][1] += 1 
            
        # Провекра поставщика на то, что он уже был рассмотрен 
        if(df["CaseID"][j] not in suppliers_info[supplier_index]):
            suppliers_info[supplier_index][2] += 1
            suppliers_info[supplier_index].append(df["CaseID"][j])
    
    # Поиск поставщика с максимальным количеством заказов
    maximum_orders = 0
    for n in range(len(suppliers_info)):
        if(maximum_orders < suppliers_info[n][2]):
            maximum_orders = suppliers_info[n][2]
    
    # Подсчет взвешенного среднего для каждого поставщика
    for r in range(len(suppliers_info)):
        if(maximum_orders == suppliers_info[r][2]):
            suppliers_info[r][3] = suppliers_info[r][1]/((maximum_orders + 1) - suppliers_info[r][2])
            continue
        suppliers_info[r][3] = suppliers_info[r][1]/(maximum_orders - suppliers_info[r][2])

    # Сортировка по взвешенному среднему
    suppliers_info.sort(key = lambda x: x[3], reverse = True)
    quantity_of_danger_sup = 0
    danger_suppliers = []
    mean_weight = suppliers_info[0][3]/2
    
    # Подсчет числа постващиков с высоким уровенем возвратов
    for g in range(len(suppliers_info)):
        if(suppliers_info[g][3] > mean_weight):
            quantity_of_danger_sup += 1
            danger_suppliers.append(suppliers_info[g][0])
        else:
            break
    
    # Подсчет количества заказов у поставщиков с высоким уровнем возвратов
    vis_cases = []
    n = 0
    quantity_of_danger_orders = 0
    for i in range(len(df["CaseID"])):
        if(df["CaseID"][i] not in vis_cases):
            if(df['Supplier'][i] in danger_suppliers):
                quantity_of_danger_orders += 1
            vis_cases.append(df["CaseID"][i])
    
    # Вычисление процента заказов у поставщиков с высоким уровнем возвратов
    return round(quantity_of_danger_orders/len(cases) * 100)
	
	

# P2P-10 Издержки возвратов
def P2P_10(df):
    
    # Создание необходимых массивов
    suppliers = df['Supplier'].unique()
    suppliers = list(suppliers)
    suppliers_list = []
    cases_summed = []

    # Создание вспомогательного списка
    for i in range(len(suppliers)):
        suppliers_list.append([suppliers[i], 0, 0, 0])
    
    # Запуск цикла по всему датафрейму
    for j in range(len(df["CaseID"])):
        
        # Поиск индекса кейса в нашем списке для фиксированного поставщика
        supplier_index = suppliers.index(df['Supplier'][j])
        if(df['CaseID'][j] not in suppliers_list[supplier_index]):
            suppliers_list[supplier_index].append(df['CaseID'][j])
            
            # Увеличение количества заказов
            suppliers_list[supplier_index][3] += 1
        
        # Поиск кейсов с возвратами
        if(df['Activity Category'][j] == 'Поступление материала-Возврат'):
            if(df['CaseID'][j] not in cases_summed):
                if(not np.isnan(df["Amount"][j])):
                    suppliers_list[supplier_index][1] += df["Amount"][j]  
                cases_summed.append(df['CaseID'][j])
                
                # Увеличение количества заказов
                suppliers_list[supplier_index][2] += 1
    
    # Реализация формулы рассчета показателя
    expec = 0
    for n in range(len(suppliers_list)):
        expec += suppliers_list[n][2]/suppliers_list[n][3] * suppliers_list[n][1]
    return expec
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	