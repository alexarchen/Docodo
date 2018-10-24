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

        

        static void Main(string[] args)
        {
   

            Console.WriteLine("Checking vocs...");
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

            Vocab[] vocs = { new Vocab(), new Vocab() };
            vocs[0].Load("Dict\\en.voc");
            vocs[1].Load("Dict\\ru.voc");


            RussianStemmer rus = new RussianStemmer();
            EnglishStemmer eng = new EnglishStemmer();

            object stemmlocker = new object();
            Func<string, string> stemm = (s) =>
             {
                 lock (stemmlocker)
                 {
                     if (s.Length <= 1) return s;
                     if (s[0] < 'z') // english
                         return (eng.Stem(s));
                     else
                         return (rus.Stem(s));
                 }
             };

            String path = "d:\\temp\\bse";
            
            Index ind = new Index("d:\\temp\\index", false, vocs, stemm);

            //ind.AddDataSource(new IndexTextCacheDataSource(new WebDataSource("web", "http://localhost/docs/reference/"),ind.WorkPath + "\\textcache.zip"));
            //ind.AddDataSource(new IndexTextCacheDataSource(new DocumentsDataSource("doc", path), ind.WorkPath + "\\textcache.zip"));
                        ind.AddDataSource(new IndexTextCacheDataSource(new IndexTextFilesDataSource("txt",path, "*.txt", 1251),ind.WorkPath+"\\textcache.zip"));
            //            ind.AddDataSource(new IndexTextFilesDataSource("txt", path, "*.txt", 1251));
            ind.bKeepForms = true;

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
                                Console.WriteLine($"  Page {p.id} ({p.number} times)");
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
