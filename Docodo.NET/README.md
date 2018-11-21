# DOCODO 
Documental full-text document search engine .NET library written on C#

Copyright (c) Alexey A. Zakharchenko
GNU GPL 3

# Features

  - Supports documents and pages
  - Search exact word positions on page
  - Search closely placed words up to exact phrase
  - Fast indexing from different sources: files, http, DB
  - Search with vocabularies and stemmers
  - Morphological and "exact" search

Библиотека .NET полнотестовый документального поиска, написанная на C#

# Характеристики
- Быстрое индексирование из различных источников документов: файлы, http, БД
- Поиск со словарями и стемматорами
- Морфологический и "точный" поиск

# Intalling

dotnet install DOCODO.NET

# Using 

 Create index object

```sh
 Index index = new Index();
```
 Add vocs, you can take vocs from https://github.com/alexarchen/Docodo/tree/master/Dict

```sh
    List<Vocab> vocs = new List<Vocab>();
    foreach (string file in Directory.GetFiles("Dict\\", "*.voc"))
     {
      vocs.Add(new Vocab(file));
     }
   index.vocs = vocs.ToArray();
   index.LoadStopWords("Dict\\stop.txt");
```
 Add datasources, for example to index c:\
 
```sh
 index.AddDataSource(new IndexTextCacheDataSource(new DocumentsDataSource("doc", "c:\\"), ind.WorkPath + "\\textcache.zip"));
```
 Now you can index


```sh
  Task ret = ind.Create();
  ret.Wait();
```
  and search

```sh
 if (index.CanSearch)
 {
  Index.SearchResult = index.Search("hello world");
 }

```