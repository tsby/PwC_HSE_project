#Загрузим библиотеки
%matplotlib inline
from collections import Counter
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import datetime
import P2PKPI
import GeneralKPI
import Anomalies
import Visualization
from copy import deepcopy #дает возможность сделать копию массива
import graphviz as gv
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.rl_config import defaultPageSize
from reportlab.platypus import PageBreak
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, cm
from reportlab.platypus.tables import Table, TableStyle
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase import ttfonts
from reportlab.lib.colors import Color
from reportlab.lib import colors


def set_table_style(table):
    table.setStyle(TableStyle([
               ('ALIGN',(0,0),(-1,-1),'CENTER'),
               ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
               ('BOX', (0,0), (-1,-1), 0.25, colors.black),
             ]))
    return table

def Make_Report(df):
	df = drop_unfinished(df)
	p2p_1 = P2P_1(df)
	p2p_2 = P2P_2(df)
	p2p_3 = P2P_3(df)
	p2p_4 = P2P_4(df)
	p2p_5 = P2P_5(df)
	p2p_6 = P2P_6(df)
	p2p_7 = P2P_7(df)
	p2p_8 = P2P_8(df)
	p2p_9 = P2P_9(df)
	p2p_10 = P2P_10(df)
	
	df = pd.read_csv('Event_log.txt', sep='\t', encoding='cp1251')
	df['Event end'] = pd.to_datetime(df['Event end'])
	
	
	order = ['Создание Заявки',
    'Заявка согласована',
    'Заявка изменена: дата доставки',
    'Заявка изменена: закупочная организация',
    'Заявка изменена: количество',
    'Заявка изменена: увеличена цена',
         
    'Заказ на поставку создан',
         
    'Требование авансового платежа',
    'Требование авансового платежа-Возврат',
    'Авансовый платеж',
    'Перерасчет по требованию авансового платежа',
    'Перерасчет авансового платежа',
    'Перерасчет авансового платежа-Возврат',
         
    'Заказ на поставку изменен: валюта',
    'Заказ на поставку изменен: налоговые условия',
    'Заказ на поставку изменен: срока предоставления скидки',
    'Заказ на поставку изменен: условия оплаты',
    'Заказ на поставку изменен: увеличено количество',
    'Заказ на поставку изменен: уменьшено количество',
    'Заказ на поставку изменен: увеличена цена',
    'Заказ на поставку изменен: уменьшена цена',
    'Заказ на поставку изменен: увеличена стоимость',
    'Заказ на поставку изменен: уменьшена стоимость',
    'Заказ на поставку изменен: эффективная стоимость',
    'Заказ на поставку изменен: группа закупки',
    'Заказ на поставку изменен: завод',
    'Заказ на поставку изменен: запланированный срок доставки в днях',
    'Заказ на поставку изменен: лимит на недопоставку',
    'Заказ на поставку изменен: лимит на сверх-поставку',
    'Заказ на поставку изменен: материал',
    'Заказ на поставку изменен: поставщик',
    'Заказ на поставку изменен: статус наличия счета',
	'Заказ на поставку изменен: тип документа',
         
    'Заказ на поставку согласован 1',
    'Заказ на поставку: изменен статус выпуска: ДанныеОтпр, возможны изменения',
    'Заказ на поставку: изменен статус выпуска: НачКод, возможны изменения',
    'Заказ на поставку: изменен статус выпуска: ДанныеСогл, измен не возможны',
    'Заказ на поставку: изменен статус выпуска: Согл, измен не возможны',
    'Заказ на поставку согласован 2',
         
    'Заказ на поставку: согласование отклонено',
    'Заявка: согласование отклонено',
    'Заказ на поставку бессрочно заблокирован',
    'Заказ на поставку удален',
    'Заявка удалена',
    'Заявка восстановлена',
    'Заказ на поставку восстановлен',
         
    'Поступление материала-Получение-Завершающая поставка',
    'Поступление материала-Получение-Частичная поставка',
    'Поступление материала-Возврат-Частичная поставка',
    'Поступление материала-Возврат-Завершающая поставка',
         
    'Счет предварительно полностью зарегистрирован',
    'Счет заведен',
    'Поступление счета',
    'Поступление счета-Возврат',
         
    'Счет блокирован: несоответствие даты',
    'Счет блокирован: несоответствие количества',
    'Счет блокирован: несоответствие цены',
    'Счет изменен: налоговые условия',
    'Счет изменен: условия оплаты',
    'Счет изменен: дата',
         
    'Платеж (выравнивание)',
         
    'Ведомость учета работ/услуг']
		 
	df['Activity'] = pd.Categorical(df['Activity'], categories=order)
	df = df.sort_values(by=['Event end', 'Activity'])
	
	tran_dict = {'Авансовый платеж': 'Upfront payment' , 
	'Авансовый платеж-Возврат': 'Upfront payment-return', 
	'Заказ на поставку':'Order for a delivery', 
	'Заказ на поставку восстановлен': 'Order for a delivery restored', 
	'Заказ на поставку изменен': 'Order for a delivery changed', 
	'Заказ на поставку отклонен': 'Order for a delivery declined',
	'Заказ на поставку согласован': 'Order for a delivery agreed',
	'Заказ на поставку создан': 'Order for a delivery created',
	'Заказ на поставку удален': 'Order for a delivery deleted', 
	'Заявка': 'Request', 
	'Заявка восстановлена': 'Request restored',
	'Заявка изменена': 'Request changed', 
	'Заявка отклонена': 'Request declined',
	'Заявка согласована': 'Request agreed',
	'Заявка удалена': 'Request deleted', 
	'Перерасчет авансового платежа': 'recalculation of upfront payment',
	'Платеж (выравнивание)': 'Payment (clearing)', 
	'Поступление материала': 'Product delivery',
	'Поступление материала-Возврат': 'Product delivery-Return', 
	'Поступление материала-Получение': 'Product delivery - Recieved',
	'Поступление счета': 'Invoice recieved', 
	'Поступление счета-Возврат': 'Invoice recieved-return',
	'Создание Заявки':'Creation of a request', 
	'Статус счета изменен': 'Status of an invoice changed',
	'Счет': 'Account',
	'Счет блокирован': 'Account blocked', 
	'Счет изменен': 'Account changed', 
	'Учет услуг': 'Accounting services'}
	
	o_1 = O_1(df)
	o_2 = O_2(df)
	o_3 = O_3(df)
	o_4 = O_4(df)
	o_5 = O_5(df)
	o_6 = O_6(df)
	o_7 = O_7(df)
	o_8 = O_8(df)
	o_9 = O_9(df)
	o_10 = O_10(df)
	
	# Вычисление аномалий
	eff_anomaly_amount, eff_anomaly_proc = eff_anomaly_detection(df)
	SOD = SOD_anomalies(df)
	time_anomaly = duration_anomaly(df)
	
	# Создание отчёта
	MyFontObject = ttfonts.TTFont('Arial', 'arial.ttf')
	pdfmetrics.registerFont(MyFontObject)

	doc = SimpleDocTemplate("Overview.pdf", pagesize=A4,
                        rightMargin=72, leftMargin=72,
                        topMargin=72, bottomMargin=18)

	p1 = ParagraphStyle(name = 'Title',
                    fontSize = 24,
                    fontName='Helvetica-Bold',
                    leftIndent = 85,
                  )

	VisualizationStyle = ParagraphStyle(name = 'Title',
                    fontSize = 20,
                    fontName='Helvetica-Bold',
                    leftIndent = 40,
                  )

	GeneralKPIStyle = ParagraphStyle(name = 'Title',
                    fontSize = 20,
                    fontName='Helvetica-Bold',
                    leftIndent = 60,
                  )

	AnomalyStyle = ParagraphStyle(name = 'Title',
                    fontSize = 20,
                    fontName='Helvetica-Bold',
                    leftIndent = 140,
                  )

	AnnexStyle = ParagraphStyle(name = 'Title',
                    fontSize = 20,
                    fontName='Helvetica-Bold',
                    leftIndent = 200,
                  )

	PKPIStyle = ParagraphStyle(name = 'Title',
                    fontSize = 20,
                    fontName='Helvetica-Bold',
                    leftIndent = 80,
                  )

	p2 = ParagraphStyle(name = 'Title',
                    fontSize = 12,
                    fontName='Arial',
                    leftIndent = 20,
                  )


	p4 = ParagraphStyle(name = 'Title',
                    fontSize = 12,
                    fontName='Arial',
                    leftIndent = 20,
                    leading = 20
                  )

	p5 = ParagraphStyle(name = 'Title',
                    fontSize = 12,
                    fontName='Arial',
                    leftIndent = 20,
                    leading = 20
                  )

	p6 = ParagraphStyle(name = 'Title',
                    fontSize = 12,
                    fontName='Arial',
                    leftIndent = 20,
                    leading = 15
                  )

	Story = []

	proc = 18
	symb = '%'


	formatted_time = time.ctime()


	ptext = 'Business process report' 
	Story.append(Paragraph(ptext, p1))
	Story.append(Spacer(1, 35))


	ptext = '1 Visualization of the business process' 
	Story.append(Paragraph(ptext, VisualizationStyle))
	Story.append(Spacer(1, 12))

	ptext = 'Detail percent of the visualization: %d %s' % (proc, symb)
	Story.append(Paragraph(ptext, p2))
	Story.append(Spacer(1, 12))



	im = Image('finalgraph.png', 7*inch, 8*inch) # width height
	Story.append(Spacer(1, 40))
	Story.append(im)

	Story.append(PageBreak())

	ptext = ' 2 General Key Performance Indicators' 
	Story.append(Paragraph(ptext, GeneralKPIStyle))
	Story.append(Spacer(1, 12))

	ptext = ' \
		<br/> 1.  Number of cases: %d <br />\
		<br/> 2.  Number of unique process paths: %d <br />\
		<br/> 3.  Number of unique users: %d <br />\
		<br/> 4.  Number of completed process paths: %d <br/>\
		<br/>     Ratio of completed process paths: %f <br />\
		<br/> 5.  Number of deviant cases based on duration: %d <br />\
		<br/>     Ration of deviant cases to total number of casess: %f <br />\
		<br/> 6.  Complicated process paths: %d <br />\
		<br/> 7.  Average number of loops per process path: %d <br />\
		<br/> 8.  Average number of cases created per day: %d <br />\
		<br/> 9.  Most popular event categories  within a business process. <br />\
		<br/> <br />' % (o_1, o_2, o_3, o_4[0], o_4[1], o_5[0], o_5[1], o_6, o_7, o_8)
	Story.append(Paragraph(ptext, p2))
	t_1 = Table(o_9[0])
	set_table_style(t_1)
	Story.append(t_1)
	Story.append(Spacer(1, 25))


	t_2 = Table(o_9[1])
	set_table_style(t_2)
	Story.append(t_2)

	ptext = '<br/> 10. Total time spent within each event class: <br />'
    
	Story.append(Paragraph(ptext, p4))
	t_3 = Table(o_10)
	set_table_style(t_3)
	Story.append(t_3)
	Story.append(Spacer(1, 35))

	Story.append(PageBreak())

	ptext = '3 P2P Key Performance Indicators' 
	Story.append(Paragraph(ptext, PKPIStyle))
	Story.append(Spacer(1, 12))

	ptext = '\
		<br/> 1.  Procurement volume: %d <br />\
		<br/> 2.  Ratio between number of goods and services: %f <br />\
		<br/> 3.  Ratio between expenditure on goods and expenditure on services: %f <br />\
		<br/> 4.  Domestic orders: %d <br />\
		<br/> 5.  Number of incomplete deliveries: %d <br />\
		<br/> 6.  Average time spent to confirm an order: %s <br />\
		<br/>     Percent of cases without approval: %d %s <br />\
		<br/>     Percent of approved cases: %d %s <br />\
		<br/> 7.  Average number of changes applied to an order: %f <br />\
		<br/>     Number of cases, in which there was at least one change applied to Purcahse Order: %d <br />\
		<br/> 8.  High risk suppliers: %d <br />\
		<br/> 9.  Number of orders carried out by high risk suppliers: %d <br />\
		<br/> 10. Expenses of returns:  %d <br />\
		' % (p2p_1, p2p_2, p2p_3, p2p_4, p2p_5, p2p_6[0], p2p_6[1], symb, p2p_6[2], symb, p2p_7[0], p2p_7[1]
			,p2p_8, p2p_9, p2p_10)
	Story.append(Paragraph(ptext, p2))
	Story.append(Spacer(1, 35))

	Story.append(PageBreak())

	ptext = '4 Anomaly analysis' 
	Story.append(Paragraph(ptext, AnomalyStyle))
	Story.append(Spacer(1, 12))

	ptext = ' \
		<br/> 1.  Number of times Segregation of Duties policies have been violated: %d <br />\
		<br/> 2.  Number of cases with abnormal run-time: % d <br />\
		<br/>     Percent of cases with abnormal run-time: %d %s <br/>\
		<br/> 3.  Number of ineffective cases: %d  <br />\
	' % (SOD[1], time_anomaly[0], time_anomaly[1], symb, eff_anomaly_amount) #, eff_anomaly_proc, symb)
	Story.append(Paragraph(ptext, p6))

	ptext =  ' \
		<br/>     Percent of ineffective cases: %d %s <br />\
	' % (int(eff_anomaly_proc * 100), symb)

	Story.append(Paragraph(ptext, p5))

	im = Image('gist.png', 5*inch, 3*inch)
	Story.append(im)
	Story.append(Spacer(1, 12))

	Story.append(Spacer(1, 35))

	Story.append(PageBreak())

	ptext = '5 Annex' 
	Story.append(Paragraph(ptext, AnnexStyle))
	Story.append(Spacer(1, 12))

	ptext = ' 1. Five most frequent process paths: ' 

	Story.append(Paragraph(ptext, p2))
	Story.append(Spacer(1, 35))

	data_1 = []

	for row in annex_1:
		s = '->'.join(row[0])
		par = Paragraph(s, p2)
		data_1.append([par, row[1]])
    
	t_4 = Table(data_1)
	set_table_style(t_4)
	Story.append(t_4)
	Story.append(Spacer(1, 35))


	ptext = '2.  Users, who have infringed Segregation of Duties policies most often:' 

	Story.append(Paragraph(ptext, p2))
	Story.append(Spacer(1, 35))

	t_5 = Table(SOD[0])
	set_table_style1(t_5)
	Story.append(t_5)
	Story.append(Spacer(1, 35))

	doc.build(Story)
	
df = pd.read_csv('Event_log.txt', sep='\t', encoding='cp1251')
Vis_Graph(df)
Make_Report(df)
	
	
	
	
	
	
	
	
	
	
	
	
	
	

