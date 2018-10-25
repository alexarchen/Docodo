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
using Iveonik.Stemmers;

namespace Docodo
{
    class Program
    {
        static private ReaderWriterLockSlim cancelLock = new ReaderWriterLockSlim();
        static private CancellationTokenSource _cancelationToken = null;
        static private CancellationTokenSource cancelationToken { get {
                cancelLock.EnterReadLock();
                try
                {
                    return (_cancelationToken);
                }
                finally
                {
                    cancelLock.ExitReadLock();
                }

            }

            set
            {
                cancelLock.EnterWriteLock();
                try
                {
                    _cancelationToken = value;
                }
                finally
                {
                    cancelLock.ExitWriteLock();
                }
            }
        }

        
        internal class VocGroup
        {
            public string root;
            public int id;
            public List<VocRecord> words = new List<VocRecord>();
        }
        internal struct VocRecord
        {
            public VocRecord(string s, VocGroup gr)
            {
                group = gr;
                suff = s;
            }
            public VocGroup group;
            public string suff;
        }


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
            if (!File.Exists("Dict\\ru.voc"))
            {
                Console.WriteLine("Creating russian voc (wait a minute)...");
                OpenCorporaVocBuilder.CreateFromOpenCorpora("Dict\\ru\\dict.opcorpora.xml", "Dict\\ru.voc");
            }
            if (!File.Exists("Dict\\en.voc"))
            {
                Console.WriteLine("Creating english voc (wait a minute)...");
                FreeLibVocBuilder.CreateFromFolder("Dict\\en", "Dict\\en.voc");
            }

            String path = "d:\\temp\\bse\\test";
            String basepath = ".";// "d:\\temp\\index";
            try
            {
                basepath = (from a in args where a.StartsWith("-i:") select a).Last().Substring(3);
            }
            catch (Exception e) { }
            ind = new Index(basepath, false, vocs.ToArray<Vocab>());
            
            //ind.AddDataSource(new IndexTextCacheDataSource(new WebDataSource("web", "http://localhost/docs/reference/"),ind.WorkPath + "\\textcache.zip"));
            ind.AddDataSource(new IndexTextCacheDataSource(new DocumentsDataSource("doc", path), ind.WorkPath + "\\textcache.zip"));
            //            ind.AddDataSource(new IndexTextCacheDataSource(new IndexTextFilesDataSource("txt",path, "*.txt", 1251),ind.WorkPath+"\\textcache.zip"));
            //            ind.AddDataSource(new IndexTextFilesDataSource("txt", path, "*.txt", 1251));
            //ind.bKeepForms = true;
            //ind.MaxDegreeOfParallelism = 1; // for test
            cancelationToken = ind.cancel; // token to cancel something

            if (ind.CanSearch) Console.WriteLine("Index loaded, contains {0} words", ind.Count());

            ConsoleKey c;
            do
            {

                Console.WriteLine("Press i to index" + (ind.CanSearch ? ", s to search" : "") + ", e to exit...");
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
                    Console.WriteLine($"Start Indexing {path}...");

                    Task ret = ind.Create();

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
                        ret.Wait();

                    }
                    catch (OperationCanceledException e)
                    {

                    }
                    cancelationToken = null;
                   

                    Console.WriteLine("Indexing complited.");

                }
            }
            while (c != ConsoleKey.E);
        }
    }

    
    

}
