# DOCODO 
Documental full-text search engine server & library written on C#

Copyright (c) Alexey A. Zakharchenko
GNU GPL 3

# Features!

  - Fast indexing from different sources: files, http, DB
  - Search with vocabularies and stemmators
  - Morphological and "exact" search
  - Console, link, RESTful interface

# Using 
## As Console app
Run application and follow instuctions
You can create index and search from console

```sh
docodo [-i:<path>]
```

## As REST server
Run application 
```sh
docodo server [-p:<port>] [-i:<path>]
```
and send search request
```sh
127.0.0.1:<port>/search?req=<request>[&params]
```
and receive pure JSON

## Us .NET library

Install DOCODO.NET package

```sh
 Index index = new Index();
 index.AddVocs(...);
 // index whole c:\\ drive and store texts internally
 index.AddDataSource(new IndexTextCacheDataSource(new DocumentsDataSource("doc", "c:\\"), ind.WorkPath + "\\textcache.zip"));
 if (index.CanSearch)
 {
  Index.SearchResult = index.Search("hello world");
 }
 else
 {
  // creating ...
  Task ret = ind.Create();
  ret.Wait();
 }

```