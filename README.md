# 4 Queries Benchmark
### Введение
В данной лабораторной работе был реализован бенчмарк ***"4 queries"*** на python для библиотек **Psycopg2,SQLite, DuckDB, SQLAlchemy, Pandas**  
Все настройки хранятся в config-файле(**JSON**), бенчмарк запускается через единственный файл ***main.py***  
В папке **"core"** хранятся файлы, обрабатывающие работу каждой из библиотек, папка **"sources"** предназначена для хранения csv файлов  
В файле **result.txt** хранятся результаты работы бенчмарка
### установка
1) Склонируйте проект на свой ПК;
2) Установите PostgreSQL;
3) Установите все необходимые библиотеки с помощью pip install -r requirements.txt;
4) Настройте файл settings.json под себя;
5) Готово :)
## Обзор используемых библиотек
### Psycopg2
<img src="https://drive.google.com/uc?export=view&id=1UX5Rmg7k8W5rR6Jp0Yg2ibY_iUCJI_tZ" width="200" height="200">  
Psycopg2 - одна из самых популярных библиотек для работы с PostgreSQL на Python. Данная библиотека основана на библиотеке libpq, написанной на С.   
Для работы с базой данных необходимо подключиться к ней и создать курсор.
Плюсы данной библиотеки(и PostgreSQL):
1) Библиотека обладает простым синтаксисом;
2) PostgreSQL во многом соответствует стандартам, поэтому к нему удобно писать запросы;
2) PostgreSQL является одной из крупнейших open-source СУБД, но при этом не уступает по возможностям проприентарным проектам;
3) Поддерживает большое число типов данных;
Минусы:
1) Серверная БД, необходимо подключаться к PostgreSQL и установить его;
2) Очень долго загружает БД из csv-файлов, потребляя коллосальное количество памяти;
### SQLite
<img src="https://drive.google.com/uc?export=view&id=17-BPqAE0O1hH6qQZHqHEi7_Tzlw8XpWn" width="200" height="200">  
SQLite - легкая встраиваемая БД. В python она уже входит в стандартную библиотеку под названием sqlite3.
Плюсы данной библиотеки:
1) Работает без подключения к серверу, все данные хранятся в файле, что облегчает перемещение и создание БД;
2) Обладает простым синтаксисом;
3) не требует отдельной установки(уже встроена в python);
Минусы:
1) Поддерживает ограниченное число типов данных и уменьшенный набор команд;
2) Работает дольше, чем другие библиотеки;
### DuckDB
<img src="https://drive.google.com/uc?export=view&id=1Oewo9mXa4-em3TlQLRY80nWpzzKWb2TU" width="200" height="200">  
DuckDB - очень быстрая библиотека для работы с базами данных.
Плюсы данной библиотеки:
1) Файловая, как и SQLite;
2) Крайне быстрая, так как обрабатывает строки не последовательно, а пользуется векторизацией и паралельными вычислениями;
3) Обладает простым синтаксисом, но при этом более продвинутым набором команд, чем SQLite;
Минусы:
1) Менее распространена, чем другие библиотеки, соответсвенно найти информацию при возникновении вопросов немного сложнее;
### SQLAlchemy
<img src="https://drive.google.com/uc?export=view&id=12ZrQQRZMQMn6P2B9VYcQaHNA4ZpJiFiy" width="200" height="200">  
SQLalchemy - библиотека, позволяющая работать с различными СУБД, а также организовывать работу с базами данных через объектно-ориентированное программирование
Плюсы данной библиотеки:
1) Позволяет организовывать работу с другими СУБД;
2) Позволяет управлять базами данных не через SQL, а методами ООП;
3) Обладает большими возможностями для работы с БД;
Минусы:
1) Довольно сложна для освоения;
### Pandas
<img src="https://drive.google.com/uc?export=view&id=18j3Z_KCXDKdr5Jlsbz1x0VBmZopNbuA4" width="200" height="200">

Pandas - одна из самых популярных библиотек для обработки и анализа данных
Плюсы данной библиотеки:
1) Построена поверх NumPy, что позволяет ей использовать векторизацию и параллельные вычисления, что занчительно ускоряет работу с данными;
2) Крайне популярна, что упрощает поиск необходимой информации;
3) Обладает большим числом функций;
Минусы:
1) Отсутствует возможность писать SQL-запросы в чистом pandas, приходится пользоваться отдельными его функциями, похожими на функционал SQL;
# Результаты
<img src="results/first.png" width="400" height="200">
<img src="results/second.png" width="400" height="200">
Было проведено 20 запусков каждого запроса для каждой библиотеки на файле размером 2 ГБ. Лучше всех себя показала DuckDB, так как 
эта библиотека написана на C++ и способна к параллельной обработке нескольких строк в таблице. Второе место заняла Pandas,
под капотом которой лежит Numpy, который также способен к векторизации, но которая менее оптимизирована для работы 
с SQL-подобными запросами. Psycopg и SQLAlchemy показывают близкие результаты, так как в SQLAlchemy PostgreSQL иcпользовался в качестве engine. 
Хуже всех себя показала легковесная библиотека SQLite, которая имеет менее оптимизирована, чем более тяжелые библиотеки, и не способна к векторизации.
