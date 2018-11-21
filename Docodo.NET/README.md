# DOCODO.NET
Documental full-text document search engine .NET library written on C#
using NET.Standard2.0

Copyright (c) Alexey A. Zakharchenko
GNU GPL 3

# Features

  - Supports documents and pages
  - Search exact word positions on page
  - Search closely placed words up to exact phrase
  - Fast indexing from different sources: files, http, DB
  - Search with vocabularies and stemmers
  - Morphological and "exact" search

Библиотека Standard полнотестового документального поиска, написанная на C#
Под .NET.Standard2.0

# Характеристики
- Поддержка страниц документов
- Поиск точных координат слов на странице
- Поиск с учетом дистанции между словами, включая поиск точной фразы
- Быстрое индексирование из различных источников документов: файлы, http, БД
- Поиск со словарями и стемматорами
- Морфологический и "точный" поиск

# Intalling for NET.Core

dotnet install DOCODO.NET

# Using 

 Create index object

```sh
 Index index = new Index();
```
 Add vocs, you can take vocs from https://github.com/alexarchen/Docodo/tree/master/Dict

```sh
    foreach (string file in Directory.GetFiles("Dict\\", "*.voc"))
     {
      index.AddVoc(new Vocab(file));
     }
   index.LoadStopWords("Dict\\stop.txt");
```
 Add datasources, for example to index c:\
 
```sh
 index.AddDataSource(new DocumentsDataSource("doc", "c:\\"));
```
 Now you can index


```sh
  await ind.CreateAsync();
```
  and search

```sh
 if (index.CanSearch)
 {
  Index.SearchResult = index.Search("hello world");
 }

```

Read Wiki for more details
