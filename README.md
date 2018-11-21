# DOCODO 
Documental full-text search engine server & library written on C#
for NET.Core 

Copyright (c) Alexey A. Zakharchenko
GNU GPL 3

# Features

  - Supports documents and pages
  - Search exact word positions on page
  - Search closely placed words up to exact phrase
  - Fast indexing from different sources: files, http, DB
  - Search with vocabularies and stemmers
  - Morphological and "exact" search

# Using 

DOCODO is NET.Core application based on DOCODO.NET NET.Standard library
https://github.com/alexarchen/Docodo/tree/master/Docodo.NET

## As Console app
Run application and follow instuctions
You can create index and search from console

```sh
docodo [-i:<path>] [-source:<source1>] [-source:<source2>] ...
```
where:
 - <path> - path to the index files, 
 - <source..> - documents source description in a form <type>,<source_path>
 where:
 -- <type> is one of doc|web|mysql,
 -- <source_path> path to the documents folder when type=doc, url of web server when type=web and pass to the query file is type=mysql
 

## As REST server
Run application 
```sh
docodo server [-p:<port>] ...
```
send search request

```sh
127.0.0.1:<port>/search?req=<request>[&params]
```
and receive pure JSON

