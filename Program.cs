//DOCODO Search Engine
//Copyright(C) 2018  Alexey Zakharchenko
// https://github.com/alexarchen/Docodo

//    This program is free software: you can redistribute it and/or modify
//    it under the terms of the GNU General Public License as published by
//    the Free Software Foundation, either version 3 of the License, or
//    (at your option) any later version.

//    This program is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
//    GNU General Public License for more details.

//    You should have received a copy of the GNU General Public License
//    along with this program.If not, see<https://www.gnu.org/licenses/>.

using System;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Text;
using Docodo;

namespace Docodo
{
    class Program
    {

        static private CancellationTokenSource cancelationToken;
    
        
        static public Index ind; // Global index
        
        static void Main(string[] args)
        {
            Console.Write("DOCODO Search Engine\nCopyrigt (c) 2018 Alexey Zakharchenko \n");
            int nPort = 9001;
            try
            {
                nPort = Int32.Parse((from a in args where a.StartsWith("-p:") select a).Last().Substring(3));
            }
            catch (Exception e) { }

            if (args.Contains("server"))
             new DocodoServer(nPort);

            //Json.JsonParser.Deserialize( parser = new Json.JsonParser();
            List<Vocab> vocs = new List<Vocab>();
            //Dictionary<string, Type> stemmers = new Dictionary<string, Type>();
            Console.Write("Loaded vocs: ");
            foreach (string file in Directory.GetFiles("Dict\\", "*.voc"))
            {
                vocs.Add(new Vocab(file));
                Console.Write(file.Substring(file.LastIndexOf("\\") + 1).Split('.')[0]+" ");
            }
            if (vocs.Count == 0) Console.Write("No!");
            Console.Write("\n");
            
            // TODO: create voc command, like -cv:en

            foreach (string crvoc in (from a in args where a.StartsWith("-cv:") select a.Substring(4)))
            {
                if (crvoc.ToLower().Equals("ru"))
                {
                    Console.WriteLine("Creating russian voc (wait a minute)...");
                    OpenCorporaVocBuilder.CreateFromOpenCorpora("Dict\\ru\\dict.opcorpora.xml", "Dict\\ru.voc");
                }
                else
                {
                    Console.WriteLine($"Creating {crvoc} voc (wait a minute)...");
                    FreeLibVocBuilder.CreateFromFolder($"Dict\\{crvoc}", $"Dict\\{crvoc}.voc");
                }
            }

            String basepath = ".";
            try
            {
                basepath = (from a in args where a.StartsWith("-i:") select a).Last().Substring(3);
            }
            catch (Exception e) { }
            ind = new Index(basepath, false, vocs.ToArray<Vocab>());
            

            foreach (string source in (from a in args where a.StartsWith("-source:") select a.Substring(8)))
            {
                var spl = source.Split(',');
                if (spl[0].Equals("doc"))
                    ind.AddDataSource(new DocumentsDataSource("doc", spl[1]));
                else
                 if (spl[0].Equals("web"))
                    ind.AddDataSource(new WebDataSource("web", spl[1]));
                else
                 if (spl[0].Equals("mysql"))
                {
                    string Connect = null;
                    string Query = null;
                    string FieldName = null;
                    string BasePath = null;
                    try
                    {
                        foreach (string line in File.ReadAllLines(spl[1]))
                        {
                            string[] name = line.Split("=");
                            if (name[0].Equals("Connect")) { Connect = line.Substring(8); }
                            if (name[0].Equals("Query")) { Query = line.Substring(6); }
                            if (name[0].Equals("BasePath")) { BasePath = line.Substring(9); }
                            if (name[0].Equals("IndexType")) { FieldName = name[1]; }
                        }

                        if (Connect == null) throw new InvalidDataException("No Connect key");
                        if (Query == null) throw new InvalidDataException("No Query key");
                        if (FieldName == null) throw new InvalidDataException("No IndexType key");
                        if (BasePath == null) throw new InvalidDataException("No BasePath key");
                        ind.AddDataSource(new MySqlDBDocSource("mysql_"+spl[1], BasePath, Connect, Query, FieldName));
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error adding mysql source: " + e.Message);
                    }
                }
            }

            String path = "d:\\temp\\bse\\test";

            //ind.AddDataSource(new IndexTextCacheDataSource(new MySqlDBDocSource("mysql", path, "server = localhost; user = root; database = Test; password = root;","SELECT * FROM Docs","Name"), ind.WorkPath + "\\textcache.zip"));

            //            ind.AddDataSource(new IndexTextCacheDataSource(new DocumentsDataSource("doc", path), ind.WorkPath + "\\textcache.zip"));
            //ind.AddDataSource(new IndexTextCacheDataSource(new WebDataSource("web", "http://localhost/docs/reference/"),ind.WorkPath + "\\textcache.zip"));
            //            ind.AddDataSource(new IndexTextCacheDataSource(new IndexTextFilesDataSource("txt",path, "*.txt", 1251),ind.WorkPath+"\\textcache.zip"));
            //            ind.AddDataSource(new IndexTextFilesDataSource("txt", path, "*.txt", 1251));
            //ind.bKeepForms = true;
            //ind.MaxDegreeOfParallelism = 1; // for test

            ind.LoadStopWords("Dict\\stop.txt");
            foreach (string sf in (from a in args
                                   where a.StartsWith("-stops:")
                                   select a.Substring(7)))
                 ind.LoadStopWords(sf);
            
            

            cancelationToken = ind.cancelationToken; // token to cancel something

            if (ind.CanSearch) Console.WriteLine("Index loaded, contains {0} words", ind.Count);
            
            ConsoleKey c;
            do
            {

                Console.WriteLine("Press "+(ind.CanIndex?"i to index, ":"") + (ind.CanSearch ? " s to search, " : "") + " e to exit...");
                c = Console.ReadKey(false).Key;

                if (c==ConsoleKey.S)
                {
                    Console.WriteLine("Type text to search, e - exit");
                    Console.Write("req:");
                    Console.InputEncoding = Encoding.Unicode;// Windows1251.GetEncoding();
                    string req;
                    while (!(req = Console.ReadLine()).Equals("e"))
                    {
                         
                        Index.SearchResult result = ind.Search(req);

                        Console.WriteLine("Found {0} pages in {1} docs:", result.foundPages.Count, result.foundDocs.Count);
                        foreach (var d in result.foundDocs)
                        {
                            Console.WriteLine($"Doc: {d.Name}, Found {d.pages.Count} pages");
                            foreach (var p in d.pages) {
                                Console.WriteLine($"  Page {p.id} ({p.pos.Count} times)");
                                Console.WriteLine("    Text: "+ p.text);
                            }
                        }
                        Console.Write("req:");
                    }




                }
                else
                if (c == ConsoleKey.I)
                {
                    Console.WriteLine($"Start Indexing ...");

                    // user input task
                   
                    Task cT = new Task(() =>
                    {
                        do
                        {
                            while (Console.In.Peek() == -1)
                            {
                                Thread.Sleep(200);
                                if (cancelationToken == null) break;
                            }

                            if (cancelationToken != null)
                            {

                                //char bc = (char); //In.Read();

                                if (Console.ReadKey().Key == ConsoleKey.C)
                                {
                                    Console.WriteLine("Indexing was interrupted by user.");
                                    cancelationToken.Cancel();
                                    break;
                                }
                            }
                        }
                        while (cancelationToken!=null);

                    });
                    //cT.Start(); // listen console to interrupt 
                    try
                    {
                        ind.Create().Wait();

                    }
                    catch (OperationCanceledException e)
                    {

                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error creaing index");
                    }
                    cancelationToken = null;
                   

                    Console.WriteLine("Indexing complited.");

                }
            }
            while (c != ConsoleKey.E);
        }
    }

    
    

}
