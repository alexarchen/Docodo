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

            //            String path = "c:\\temp";
            String path = "d:\\temp\\bse\\test";

            RussianStemmer rus = new RussianStemmer();
            EnglishStemmer eng = new EnglishStemmer();

            Func<string, string> stemm = (s) =>
             {
                 if (s.Length <= 1) return s;
                 if (s[0] < 'z') // english
                     return (eng.Stem(s));
                 else
                     return (rus.Stem(s));
                 
             };

            // var enc1252 = CodePagesEncodingProvider.Instance.GetEncoding(1251);

         
            Index<ByteString> ind = new Index<ByteString>("d:\\temp\\index", false, vocs, stemm);

            cancelationToken = ind.cancel; // token to cancel something

            if (ind.CanSearch) Console.WriteLine("Index loaded, contains {0} words", ind.Count());

            char c;
            do
            {

                Console.WriteLine("Press i to index" + (ind.CanSearch ? ", s to search" : "") + ", e to exit...");
                c = Console.ReadKey().KeyChar;

                if (c == 's')
                {
                    Console.WriteLine("Type text to search, e - exit");
                    Console.Write("req:");
                    Console.InputEncoding = Encoding.Unicode;// Windows1251.GetEncoding();
                    string req;
                    while (!(req = Console.ReadLine()).Equals("e"))
                    {
                         
                        Index<ByteString>.SearchResult result = ind.Search(req);

                        Console.WriteLine("Found {0} pages in {1} docs:", result.foundPages.Count, result.foundDocs.Count);
                        foreach (Index<ByteString>.ResultDocPage p in result.foundPages)
                            Console.WriteLine("Page {0} in {1} ({2} times)", p.id, p.doc.Name, p.number);

                        Console.Write("req:");
                    }




                }
                else
                if (c == 'i')
                {
                    Console.WriteLine($"Start Indexing {path}...");

                    IIndexDataSource[] sources = new IIndexDataSource[] { new IndexTextFilesDataSource(path, "*.txt",1251) };
                    Task ret = ind.CreateBy(sources);

                    // user input task
                    Task cT = new Task(() =>
                    {
                        do
                        {
                            char bc = Console.ReadKey().KeyChar;

                            if (cancelationToken != null)
                            {
                                if (bc == 'c')
                                {
                                    Console.WriteLine("Indexing was interrupted by user.");
                                    cancelationToken.Cancel();
                                }
                            }
                            else
                            {
                                Console.WriteLine("Exiting...");
                                break;
                            }
                        }
                        while (true);

                    });
                    cT.Start();
                    try
                    {
                        ret.Wait();

                    }
                    catch (OperationCanceledException e)
                    {

                    }
                    cancelationToken = null;
                    foreach (IIndexDataSource sourse in sources) sourse.Dispose();

                    Console.WriteLine("Indexing complited. Press any key...");

                }
            }
            while (c != 'e');
        }
    }

    
    

}
