# Visualization

Vis_Graph(df):
	#Вычисление оптимального процента детализации графа бизнес-процесса

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

	#Обработка данных: группировка событий в цепочки
	cases = df.groupby(['CaseID'])['Activity'].apply(lambda x: x.sum())
	cases = cases.reset_index()
	casesun = cases.drop_duplicates(subset=['Activity'])
	casesun['Newcount'] = casesun['Activity'].map(cases['Activity'].value_counts())
	caseval = casesun['CaseID']
	arrcase = df.groupby(['CaseID'])['Activity'].apply(list)
	arrcase = arrcase.reset_index()
	caseval = np.array(caseval)
	arrcaseun = arrcase.loc[arrcase['CaseID'].isin(caseval)]

	#Кластеризация: подготовка признаков
	arrcaseun['cluster'] = 0
	arrcaseun['unique'] = arrcaseun['Activity'].agg(pd.unique)
	arrcaseun['numactivities'] = arrcaseun['unique'].apply(lambda x: len(x))
	arrcaseun['reps'] = casesun['Newcount']

	#Кластеризация: реализация итеративного метода аномального кластера
	cur = arrcaseun[arrcaseun['cluster'] == 0]['numactivities'] #Кейсы, не принадлежащие аномальному кластеру, на текущей итерации
	i = 0
	therearenans = False
	while (len(cur) != 0):
		i = i + 1
		distances = np.zeros((len(cur),2)) #Разница между количеством событий в текущей цепочке и средним количеством событий 
										   #оставшихся цепочек
                                           #Разница между текущим количеством событий в цепочке и максимальным количеством событий 
                                           #по всем оставшимся цепочкам
		centers = np.zeros(2) #Среднее значение количества событий в текущих цепочках
                              #Максимальное значение количества событий в текущих цепочках
		flag = True
		zero = cur.mean()
		centers[0] = zero 
		s = cur - zero
		center = s.idxmax(s.apply(lambda x: np.linalg.norm(x)))
		centers[1] = cur[center]
		centers_old = np.zeros(centers.shape) #Среднее значение количества событий в цепочках, рассматриваемых на предыдущей итерации
                                              #Максимальное значение количества событий в цепочках, рассматриваемых на предыдущей итерации
		clusterser = pd.Series([])
		if (therearenans == True):
			break
		while flag == True:
			distances[:,0] = (cur - centers[0]).apply(lambda x: np.linalg.norm(x))
			distances[:,1] = (cur - centers[1]).apply(lambda x: np.linalg.norm(x))
			clusters = np.argmin(distances, axis = 1)
			clusterser = pd.Series(clusters)
			clusterser.index = cur.index
			centers_old = deepcopy(centers)
			centers[0] = np.mean(cur[clusters == 0], axis=0)
			centers[1] = np.mean(cur[clusters == 1], axis=0)
			error = np.linalg.norm(centers - centers_old)
			if(np.isnan(centers).any() == True):
				flag = False
				therearenans = True
			if (error == 0):
				flag = False
		new = clusterser[clusterser == 1]
		arrcaseun['cluster'][new.index] = i
		cur = arrcaseun[arrcaseun['cluster'] == 0]['numactivities']
	arrcaseun = arrcaseun.drop(cur.index, axis = 0)
	maxnumclust = max(arrcaseun['cluster']) #Количество аномальных кластеров

	#Кластеризация: отбор кластеров для визуализации
	qorclust = [] #Массив, содержащий количество  кейсов в кластере
	i = 1
	while i <= maxnumclust:
		clust_i = arrcaseun['reps'][arrcaseun['cluster'] == i]
		qorclust.append(sum(clust_i))
		i += 1
	qormin = min(qorclust)
	qormax = max(qorclust)
	def f1(qormin, qormax, qor): #Функция нормирования количества кейсов в кластере
		return (qor - qormin)/(qormax-qormin)
	T = [] #Массив, содержащий количество уникальных верщин в каждом кластере, которые не были рассмотрены на предыдущей итерации
	i = 2
	prev = set(arrcaseun['unique'][arrcaseun['cluster'] == 1].apply(pd.Series).stack().value_counts().index) #Множество вершин, содержащихся в предыдущих кластерах
	T.append(len(prev)) 
	while i <= maxnumclust:
		cur = arrcaseun['unique'][arrcaseun['cluster'] == i].apply(pd.Series).stack().value_counts()
		unelems = set(cur.index)
		newelems = unelems - prev #Вершины, которые не лежали в предыдущих кластерах
		T.append(len(newelems))
		prev = prev | newelems #Добавление нерасмотренных вершин во множество рассмотренных
		i += 1
	arrcaseun['vis'] = 0
	arrcaseun['vis'][arrcaseun['cluster'] == 1] = 1
	def g1(T_min, T_max, t): #Функция нормирования количества кейсов в кластере
		return (t - T_min)/(T_max-T_min)
	Tmin = min(T)
	Tmax = max(T)
	i = 2
	visclust = [] #Массив, содержащий количество кейсов в каждом из визуализированных кластеров
	visvert = [] #Массив, содерджащий количество уникальных вершин в каждом из визуализированных кластеров
	visclust.append(qorclust[0])
	visvert.append(T[0])
	while((f1(qormin, qormax, qorclust[i-1]) + g1(Tmin, Tmax, T[i-1])) >(f1(qormin, sum(qorclust), sum(visclust)) + g1(Tmin, sum(T), sum(visvert)))): 
		arrcaseun['vis'][arrcaseun['cluster'] == i] = 1
		visclust.append(qorclust[i-1])
		visvert.append(T[i-1])
		i+=1

	#Подготовка к визуализации: отбор кейсов для визуализации
	visualize = arrcaseun[arrcaseun['vis'] == 1]

	#Подготовка к визуализации: удаление двоеточий в названиях событий и уникальных событий для корректной работы библиотеки визуализации
	for i in (visualize['Activity'].index):
		visualize['Activity'][i] = [w.replace(':', '') for w in visualize['Activity'][i]]
	for i in (visualize['unique'].index):
		visualize['unique'][i] = [w.replace(':', '') for w in visualize['unique'][i]]
    
	#Подготовка к визуализации: отбор вершин для визуализации
	vertices = visualize['unique'].apply(pd.Series).stack().value_counts().index

	arrs = []

	#Подготовка к визуализации: создание множества ребер графа
	for i in visualize['Activity'].index:
		arrs.append(list(zip(visualize['Activity'][i][:-1], visualize['Activity'][i][1:])))
    

	#Подготовка к визуализации: удаление параллельных ребер между двумя вершинами
	edges = set(arrs[0])
	for i in range(len(arrs[:int(len(arrs)/30)])):
		edges = edges.union(arrs[i])
    
	#Подготовка к визуализации: построение графа
	graph = gv.Digraph(format="png")
	graph.node_attr.update(color = 'lightblue', style = 'filled', fixedsize = 'true')
	graph.attr(size='40,12')
	for edge in edges:
		graph.node(edge[0],label= edge[0],  shape="circle", color="#F3BE26")
		graph.node(edge[1], label=edge[1], shape="circle", color="#F3BE26")
		graph.edge(edge[0], edge[1])
	graph.render("finalgraph")
	
	
