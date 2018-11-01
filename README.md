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
docodo [-i:<path>] [-source:<source1>] [-source:<source2>] ...
```
where <path> - path to the index files, 
<source..> - documents source description in a form <type>,<source_path>
 where <type> is one of doc|web|mysql,
 <source_path> path to the documents folder when type=doc, 
 url of web server when type=web and pass to the query file is type=mysql
 

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

