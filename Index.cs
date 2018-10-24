using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using DynamicExpresso;
using System.Text;
using Iveonik.Stemmers;
using System.Text.RegularExpressions;
using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace Docodo
{


    class Index : SortedList<string, IndexSequence> 
    {
        const int MAX_DEF_TMP_INDEXITEMS = 1000000; // Maximum items in tempindex
        const int MAX_WORD_LENGTH = 32;      // Maximum word length
        const long MAX_FILE_SIZE = 200000000; // Maximum indexable text file size
        const int COORD_DEVIDER = 1; // devider of coordinates in index
        const int SUBWORD_LENGTH = 4; // words in index are splittered by groups of this number of chars
        const int MAX_FOUND_PAGES = 1000; // maximum output found pages
        const int MAX_FOUND_PAGES_IN_DOC = 100; // maximum output found pages in one document

        /* Index constructor
         *   path - working folder, if set it will load 
         *            autmatically, else set WorkPath before call to CreateBy
         *   InMemory - if true load into memmory, else on disk         
         *   stemmer - stemmer function to extract word base 
         */
         public IndexSequence GetTest(ulong i)
         {
            return new IndexSequence((new ulong[] { i }));
         }

        public Index(string path, bool InMemory, Vocab []vocs=null) : base()
        {
          
            MaxDegreeOfParallelism = 2;
            MaxTmpIndexItems = MAX_DEF_TMP_INDEXITEMS;
            WorkPath = path;
            if (path.Length > 0)
                Load(path);
            
            this.InMemory = InMemory;
            this.vocs = vocs;
        }

        public bool InMemory { get; private set; } = false; /* Load into memory or leave on disk */
        public bool bKeepForms { get; set; } = false;/* Keep full forms of the words */
        public bool CanSearch { get; private set; } = false; /* If index is loaded */

       
   
        public class SearchResult
        {
            public HashSet<ResultDocument> foundDocs = new HashSet<ResultDocument>();
            public List<ResultDocPage> foundPages = new List<ResultDocPage>();
        }
        public class ResultDocument: Document
        {
            public ResultDocument() : base() { }
            public ResultDocument(Document d) : base(d.Name,d.nPages) { }
            public ResultDocument(string s, int n=0) : base(s,n) { }
            public HashSet<ResultDocPage> pages=new HashSet<ResultDocPage>();
        }

        public class ResultDocPage : DocPage
        {
            public ResultDocPage(DocPage p) : base(p.doc, p.id)
            {

            }
            public int number { get => pos.Count; } // number of found
            public List<int> pos = new List<int>(); // positions on the page
            public string text; //surrounding text
        }

        private static string FromInt(int i) { return ("#" + Convert.ToBase64String(new byte[] { (byte)((i >> 24) & 0xFF), (byte)((i >> 16) & 0xFF), (byte)((i >> 8) & 0xFF), (byte)((i) & 0xFF) }).TrimEnd('=')); }

        private IndexSequence LoadSequence(IndexSequence seq)
        {
            if (reader != null)
            {
                reader.BaseStream.Seek((long)seq[1], SeekOrigin.Begin);
                int len = (int)seq[0];
                byte[] buffer = new byte[len * 4];
                reader.BaseStream.Read(buffer, 0, len * 4);
                uint[] arr = new uint[len];
                Buffer.BlockCopy(buffer, 0, arr, 0, len*4);
                return (new IndexSequence(arr));

            }
            return (null);
        }

        private StreamReader reader = null;
        public Vocab[] vocs { get; set; } = null;

        private void Close()
        {
            CanSearch = false;

            if (reader != null)
                reader.Close();
            reader = null;

        }

        ~Index()
        {
            Close();

        }
        public IndexSequence this[string key]
        {
           get {
            IndexSequence seq = base[key];
                if (!InMemory)
                    seq = LoadSequence(seq);
            return (seq);
            }
        }

        private IndexSequence SearchWord(string word)
        {
            if (word.Length <= 2) return (new IndexSequence(/* Empty */));
            string stemmed = word;
            

            IndexSequence res=null;
            try
            {
                int nG = 0;
                int nVoc = 0;
                foreach (Vocab voc in vocs)
                {
                    // TODO: detect prefered language
                    if ((voc != null) && (word[0] >= voc.Range[0]) && (word[0] <= voc.Range[1]))
                    {
                        stemmed = voc.Stem(word);
                        nG = voc.Search(stemmed);
                        if (((nG & Vocab.GROUP_NOT_EXCACT_WORD_MASK) != 0) && (stemmed.Equals(word))) nG = 0;
                        if (nG != 0) break;
                    }
                    nVoc++;
                }
                if (nG != 0)
                {
                    string str = FromInt((nVoc<<24) | (nG & Vocab.GROUP_NUMBER_MASK));
                    return (this[str]);

                }
                else
                {
                    word = "<" + word + ">";

                    for (int q = 0; q < word.Length; q += SUBWORD_LENGTH)
                    {
                        if (res == null) res = this[word.Substring(q, Math.Min(word.Length - q, SUBWORD_LENGTH))];
                        else res *= this[word.Substring(q, Math.Min(word.Length - q, SUBWORD_LENGTH))];
                        res.R = -(q + 1);
                    }
                    IndexSequence newres = new IndexSequence();
                    if (res.Count > 1)
                    {
                        ulong prevc = res[0];
                        newres.Add(prevc);
                        foreach (ulong c in res)
                        {
                            if (c - prevc > (ulong)Math.Abs(res.R)) newres.Add(c);
                            prevc = c;
                        }
                    }
                    return (newres);
                }

            }
            catch (Exception e)
            {
                res = new IndexSequence(/* Empty */);
            }
            return (res);
        }

        public class SearchOptions
        {
            public int dist; // max distance between words in letters

        }

        public SearchResult Search(string req, SearchOptions opt=null)
        {

            //using System.Linq.Dynamic, 
//            Expression expr = System.Linq.Dynamic.DynamicExpression.Parse(typeof(IndexSequence), "GetTest(1) * GetTest(2)", null);
//            LambdaExpression e = Expression.Lambda(expr);
//            IndexSequence tst = ((Func<IndexSequence>)e.Compile())();

            try
            {
                lock (DoSearchLock) // wait antil can search
                {

                    var interpreter = new Interpreter();

                    req = req.ToLower();

                    // search filter
                    List<string> filter = new List<string>();
                    var matches = Regex.Match(req, @"\B-filter:([\w\*\?\\.()+{}/]+,?)+");
                    if (matches.Groups.Count > 1) {
                        foreach (var cap in matches.Groups[1].Captures)
                        {
                            
                            filter.Add(cap.ToString().Trim(','));
                        }
                    }
                    req = Regex.Replace(req, @"\B-filter:([\w\*\?\\.()+{}/]+,?)+", " ");

                    req = Regex.Replace(req, @"\b\w{1,2}\b", " ");
                    req = Regex.Replace(req, @"\b(^\w|[()])+\b", " ");
                    req = Regex.Replace(req, @"\b(\w+)\b", "Get(\"${0}\") * ");
                    req = req.TrimEnd(new char[] { ' ', '*' });

                    int R = 255;
                    if (opt != null) R = opt.dist;

                    Func<string, IndexSequence> Get = (word) => { IndexSequence seq = SearchWord(word); seq.R = Math.Sign(R) * (word.Length / SUBWORD_LENGTH + Math.Abs(R)); return (seq); };
                    interpreter.SetFunction("Get", Get);

                    IndexSequence res = interpreter.Eval<IndexSequence>(req); ;


                    if (res != null)
                    {


                        Console.WriteLine("Found!");
                        SearchResult result = new SearchResult();
                        DocPage prevp = null;
                        Document prevd = null;
                        ResultDocPage lastResultDocPage = null;
                        ResultDocument lastDoc = null;
                        foreach (ulong coord in res)
                        {

                            DocPage _p = PagesList.GetPage(coord);
                            if (!_p.Equals(prevp))
                            {
                                lastResultDocPage = new ResultDocPage(_p);
                                lastResultDocPage.pos.Add((int)(coord - _p.coord));
                                result.foundPages.Add(lastResultDocPage);
                                if (prevd != _p.doc)
                                {
                                   ResultDocument doc = new ResultDocument(_p.doc);
                                    // check filter
                                    bool matched =  filter.Count==0;
                                    foreach (string filt in filter)
                                    {
                                        if (Regex.Match(doc.Name, filt).Success) { matched = true; break; }
                                    }
                                    if (matched)
                                     result.foundDocs.Add(doc);
                                   lastDoc = doc;
                                }
                                lastResultDocPage.doc = lastDoc;
                                lastDoc.pages.Add(lastResultDocPage);
                                prevp = _p;
                                prevd = _p.doc;
                            }
                            else
                                lastResultDocPage.pos.Add((int)(coord - _p.coord));

                            if (result.foundPages.Count > MAX_FOUND_PAGES) break;

                        }
                        // retrieve surrounding text
                        
                        foreach (var doc in result.foundDocs){
                            foreach (var source in sources)
                             if (source.Name.Equals(doc.Name.Split(':')[0])) 
                                {
                                    if ((source!=null) && (source is IIndexDirectDataSource))
                                    {
                                        IIndexDirectDocument document = (source as IIndexDirectDataSource)[doc.Name.Substring(doc.Name.Split(':')[0].Length + 1)];
                                        if (document != null)
                                        {
                                            foreach (var page in doc.pages)
                                            {
                                                string text = document[page.id].text;
                                                int[] Range = { 0, 0 };
                                                Range[0] = Math.Min(Math.Max(0, page.pos.Min() - 64), text.Length);
                                                Range[1] = Math.Min(Math.Min(page.pos.Max() + 64, text.Length), Range[0] + 256);

                                                page.text = PreparePageText(text.Substring(Range[0], Range[1] - Range[0]));


                                            }
                                            document.Dispose();
                                        }
                                    }
                                    break;
                                }
                            
                        }


                        return result;

                    }
                }

            }
            catch (Exception e)
            {

            }

            Console.WriteLine("Not fond!");
            return (new SearchResult());
        }
        private static string PreparePageText(string text)
        {
            text = Regex.Replace(text, @"\b\W*\.+\W*\b", ". ");
            text = Regex.Replace(text, @"\b\W*\?+\W*\b", "? ");
            text = Regex.Replace(text, @"\b\W*!+\W*\b", "! ");
            text = Regex.Replace(text, @"\b\W*:+\W*\b", ": ");
            text = Regex.Replace(text, @"\b\W*,+\W*\b", ", ");
            text = text.Replace("\r", " ");
            text = text.Replace("\n", " ");
            return (text);

        }

        public bool Load(string path)
        {
            if ((File.Exists(path + "\\" + ".index")) && (File.Exists(path + "\\" + ".index.list")))
            {
                CanSearch = false;
                Clear();

                try
                {
                    BinaryReader bin = new BinaryReader(File.OpenRead(path + "\\" + ".index"));


                    try
                    {
                        bin.ReadUInt64();
                        do
                        {
                            string s = bin.ReadString();
                            int n = bin.ReadInt32();

                           
                            if (n > 0)
                            {
                                if (InMemory)
                                {
                                    uint[] arr = new uint[n];
                                    byte[] bytes = bin.ReadBytes(n * sizeof(int));
                                    Buffer.BlockCopy(bytes, 0, arr, 0, n * sizeof(uint));
                                    Add(s, new IndexSequence(arr));
                                }
                                else
                                {
                                    Add(s, new IndexSequence(new ulong[] { (ulong)n, (ulong)bin.BaseStream.Position }));
                                    bin.BaseStream.Seek(n * sizeof(uint), SeekOrigin.Current);
                                }
                            }
                            else throw new InvalidDataException();

                        }
                        while (true);
                    }
                    catch (EndOfStreamException e)
                    {

                    }

                    if (!InMemory) { reader = new StreamReader(bin.BaseStream); }
                    else bin.Close();

                    bin = new BinaryReader(File.OpenRead(path + "\\" + ".index.list"));

                    PagesList = new IndexPageList();
                    PagesList.Load(bin);
                    bin.Close();

                    CanSearch = true;
                    return (true);

                }
                catch (Exception e)
                {

                }
            }
            return (false);
        }

        public void Cancel()
        {
            cancel.Cancel();
        }


        private ParallelOptions po = new ParallelOptions();
        public CancellationTokenSource cancel { get; private set; }
        public int MaxDegreeOfParallelism { get => po.MaxDegreeOfParallelism; set => po.MaxDegreeOfParallelism = value; }

        public enum Status
        {
            Idle, Nav, Index, Merge
        };

        public Status status { get; private set; }

        private object DoSearchLock = new object();

        private IIndexDataSource[] sources;

        public void AddDataSource(IIndexDataSource source)
        {
            if (sources == null) { sources = new IIndexDataSource[1]; sources[0] = source; }
            else
            {
             Array.Resize(ref sources, sources.Length + 1);
             sources[sources.Length - 1] = source;
            }
            
        }

        public Task Create()
        {
            if ((sources==null) || (sources.Length==0) || (sources[0]==null)) return null;

            if (status == Status.Idle)
            { //first time
                //Close();
                long startTime = Environment.TickCount;

                status = Status.Nav;
                cancel = new CancellationTokenSource();
                po.CancellationToken = cancel.Token;

                return (Task.Factory.StartNew(() =>
                {
                    try
                    {

                        if (Directory.Exists(WorkPath))
                            foreach (string dir in Directory.GetDirectories(WorkPath))
                                Directory.Delete(dir, true);
                        else
                         Directory.CreateDirectory(WorkPath);


                        List<Task> tasks = new List<Task>();


                        foreach (IIndexDataSource source in sources)
                        {
                            source.Reset();
                            for (int q = 0; q < Math.Max(1,MaxDegreeOfParallelism); q++)
                                tasks.Add(Task.Factory.StartNew(() => { IndexTask(source); }, po.CancellationToken));
                        }

                        status = Status.Index;
                        Task.WaitAll(tasks.ToArray(), po.CancellationToken);
                        Console.WriteLine("Index finished");

                        foreach (IIndexDataSource source in sources)
                            source.Dispose();

                            // Next parallel Merge 
                            status = Status.Merge;
                        foreach (string dir in Directory.GetDirectories(WorkPath))
                            MergeAll(dir);
                    // Overall Merge
                    Console.WriteLine("Final merge...");
                        ArrayList files = new ArrayList();
                        foreach (string dir in Directory.GetDirectories(WorkPath))
                        {
                            string[] f = Directory.GetFiles(dir, "*.tmpind");
                            if (f.Length > 0) files.Add(f[0]);
                        }

                        MergeIndexes((String[])files.ToArray(typeof(string)), WorkPath + "\\" + ".index");
                        var numbers = new[] { 1, 2, 3, 4, 5 };

                        Array.ForEach(Directory.GetDirectories(WorkPath), i => Directory.Delete(i, true));

                        status = Status.Idle;

                        Console.WriteLine("Time elasped: {0} s",(Environment.TickCount - startTime) / 1000);
                    }
                    catch (Exception e)
                    {
                    //Cancel();
                }
                    cancel = null;
                }, cancel.Token));

            }

            return (Task.Factory.StartNew(() => { /* empty */ }));


        }

        /* Merge all files in subfolders */
        private void MergeAll(string Path)
        {
            try
            {
                string[] files;
                do
                {

                    files = Directory.GetFiles(Path, "*.tmpind");
                    if (files.Length > 1)
                    {
                        int nMax = 5;
                        for (int q = 0; q < files.Length - 1; q += nMax)
                        {
                            string[] newfiles = new string[Math.Min(nMax, files.Length - q)];
                            Array.Copy(files, q, newfiles, 0, newfiles.Length);
                            MergeFiles(newfiles, newfiles[0] + "_");
                            Console.WriteLine("Merge: " + newfiles[0] + " + " + newfiles[1]);
                            foreach (string f in newfiles) File.Delete(f);
                            File.Move(newfiles[0] + "_", newfiles[0]);
                            File.Delete(newfiles[0] + "_");

                        }

                    }

                } while (files.Length > 1);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error merging files: ", e.Message);
                this.Cancel();

            }
            // now we have 1 .ind file for each folder

        }

        /* Merges tmpindex in files together */
        /* output to fname */
        private void MergeFiles(string[] files, string fname, bool shift_coords = false)
        {
            try
            {

                BinaryReader[] bins = new BinaryReader[files.Length];

                ulong[] maxCoords = new ulong[files.Length];
                ulong[] shifts = new ulong[files.Length];
                shifts[0] = 0;// first index have always 0 shift
                for (int q = 0; q < files.Length; q++)
                {
                    bins[q] = new BinaryReader(File.OpenRead(files[q]));

                    maxCoords[q] = bins[q].ReadUInt64();
                    if (q > 0) shifts[q] = shifts[q - 1] + maxCoords[q - 1];
                }

                BinaryWriter wr = new BinaryWriter(File.Create(fname));
                wr.Write(maxCoords.Max());


                string[] s = new string[files.Length]; // next words
                Array.Fill(s, " ");
                int[] n = new int[files.Length]; // numbers of coords in vectors
                uint[][] arr = new uint[files.Length][]; // coord arrays
                bool[] readnext = new bool[files.Length]; // what index read next
                Array.Fill(readnext, true); // need to read from each index first

                var s_end = from str in s where str.Length != 0 select str;
                
                do
                {
                    for (int q = 0; q < files.Length; q++)
                        if ((readnext[q]) && (s[q].Length > 0))// read next string & array if previous was read
                        {
                            try
                            {
                                s[q] = bins[q].ReadString();
                                n[q] = bins[q].ReadInt32();
                                arr[q] = new uint[n[q]];

                                byte[] bytes = bins[q].ReadBytes(sizeof(uint) * n[q]);
                                Buffer.BlockCopy(bytes, 0, arr[q], 0, sizeof(uint) * n[q]);
                            }
                            catch (EndOfStreamException e)
                            {
                                s[q] = "";
                            }

                        }
                    // define next step
                    List<KeyValuePair<string, int>> list = new List<KeyValuePair<string, int>>();
                    for (int q = 0; q < files.Length; q++)
                        if (s[q].Length > 0) // was previously read
                            list.Add(new KeyValuePair<string, int>(s[q], q));
                    list.Sort(new Comparison<KeyValuePair<string, int>>((a, b) =>
                    {
                    // compare by words and then by coordinates
                    int i = a.Key.CompareTo(b.Key);
                        if (i == 0)
                        {
                            if (shift_coords) return (maxCoords[a.Value].CompareTo(maxCoords[b.Value]));
                            else return (shifts[a.Value].CompareTo(shifts[b.Value]));
                        }
                        return (i);
                    }));
                    Array.Fill(readnext, false);
                    if (list.Count > 0)
                    {
                        readnext[list[0].Value] = true;
                        int nsize = n[list[0].Value];

                        for (int q = 1; q < list.Count; q++)
                            if (list[q].Key.Equals(list[q - 1].Key))
                            {
                                readnext[list[q].Value] = true;
                                nsize += n[list[q].Value];
                            }

                        wr.Write(s[list[0].Value]);
                        wr.Write(nsize);
                        for (int q = 0; q < files.Length; q++)
                            if (readnext[q])
                            {
                                // write coord array
                                //TODO!!!! For large bases  (uint)(shifts[q]) doen't work 
                                if (shift_coords) { for (int w = 0; w < arr[q].Length; w++) arr[q][w] += (uint)(shifts[q]); }
                                byte[] arrbytes = new byte[n[q] * sizeof(int)];
                                Buffer.BlockCopy(arr[q], 0, arrbytes, 0, n[q] * sizeof(int));
                                wr.Write(arrbytes);
                            }

                    }

                } while (s_end.Count() > 0);


                foreach (BinaryReader bin in bins) bin.Close();
                wr.Close();

            }
            catch (Exception e)
            {
                Console.WriteLine($"Error merging: " + e.Message);
            }

        }

        /* Merge different indexes and file lists => one index*/
        /* files[i] is path to .tmpind file,
         * correcponing .list file present
        /* Result index is stored in one file output*/
        public void MergeIndexes(string[] files, string output)
        {
            if (files.Length == 0) return;
            // merge in temp file first
            MergeFiles(files, output+"_", true); 
            PagesList = new IndexPageList();

            // merge lists
            try
            {
                ulong[] shifts = new ulong[files.Length];
                shifts[0] = 0;
                //            Dictionary<string, int> result = new Dictionary<string, int>();
                ulong m = 0;
                for (int q = 0; q < files.Length; q++)
                {
                    if (q > 0) shifts[q] = shifts[q - 1] + m;
                    BinaryReader bin = new BinaryReader(File.OpenRead(files[q]));
                    m = bin.ReadUInt64();
                    bin.Close();

                    BinaryFormatter binf = new BinaryFormatter();
                    FileStream f = File.OpenRead(new FileInfo(files[q]).DirectoryName + "\\index.tmplist");

                    List<KeyValuePair<string, ulong>> dict = (List<KeyValuePair<string, ulong>>)binf.Deserialize(f);
                    f.Close();


                    Document doc = new Document();

                    foreach (KeyValuePair<string, ulong> d in dict)
                    {
                        if (d.Key[0] != ':') // doc
                        {
                            doc = new Document(d.Key);
                            PagesList.AddDocument(doc);
                        }
                        else
                        {
                            PagesList.Add(d.Value+shifts[q], new DocPage(doc, d.Key.Substring(1)));
                        }


                    }
                }

                lock (DoSearchLock) // stop all seach tasks before replacing index
                {
                    if (File.Exists(output + ".list")) File.Delete(output + ".list");
                    BinaryWriter binOut = new BinaryWriter(File.Create(output + ".list"));
                    PagesList.Save(binOut);
                    binOut.Close();

                    
                    Close();
                    if (File.Exists(output)) File.Delete(output);
                    File.Move(output + "_", output);

                    Load(new FileInfo(output).DirectoryName);

                }


                foreach (string file in files) File.Delete(file);


            }
            catch (Exception e)
            {
                Console.WriteLine("Error mering indexes: " + e.Message);
            }

            if (File.Exists(output + "_"))
             File.Delete(output+"_");
        }


        public virtual bool IsLetter(char l)
        { // English language only, override to add new one
            if ((l >= 'a') && (l <= 'z')) return (true);
            if ((l >= 'а') && (l <= 'я')) return (true);
            if ((l >= '0') && (l <= '9')) return (true);
            if (l == '\'') return (true);
            return (false);
        }



        public int MaxTmpIndexItems { get; set; }

        public class Document
        {
            public string Name;
            public int nPages;
            public Document() { }
            public Document(string a, int n = 0) { Name = a; nPages = n; }
        }
        public class DocPage
        {
            public string id;
            public Document doc;
            public ulong coord; // base coordinate to calculate pos
            public DocPage() { }
            public DocPage(Document _doc, string _id) { doc = _doc; id = _id; }
        }

        class IndexPageList : Dictionary<ulong, DocPage>
        {
            List<Document> DocsList = new List<Document>();
            public void AddDocument(Document doc)
            {
                DocsList.Add(doc);
            }

            IEnumerator<KeyValuePair<ulong, DocPage>> _enum;

            ulong prevc = 0xFFFFFFFF;

            public DocPage GetPage(ulong coord)
            {


                if (prevc == 0xFFFFFFFF) { _enum = this.AsEnumerable().GetEnumerator(); _enum.MoveNext(); prevc = 0; }
                else
                {
                    if (_enum.Current.Key == coord) return (_enum.Current.Value);
                    if ((_enum.Current.Key > coord) && (prevc <= coord)) return _enum.Current.Value;
                    if (_enum.Current.Key > coord) { _enum = this.AsEnumerable().GetEnumerator(); _enum.MoveNext(); prevc = 0; }
                }
                // do next iteration
                do
                {
                    if (_enum.Current.Key > coord)
                    {
                        _enum.Current.Value.coord = prevc;
                        return _enum.Current.Value;
                    }
                    prevc = _enum.Current.Key;
                } while (_enum.MoveNext());

                return (new DocPage());
            }

            public void Save(BinaryWriter bin)
            {
                if (this.Count() == 0) return;
                Document doc = new Document();
                foreach (KeyValuePair<ulong, DocPage> p in this)
                {
                    bin.Write(p.Key);
                    if (!doc.Equals(p.Value.doc))
                    {
                        doc = p.Value.doc;
                        bin.Write(doc.Name);
                        bin.Write(p.Key);
                        bin.Write(":" + p.Value.id);
                    }
                    else
                    {
                        bin.Write(":" + p.Value.id);
                    }
                }
            }
            public void Load(BinaryReader read)
            {
                DocsList = new List<Document>();

                try
                {
                    Document doc = new Document();
                    do
                    {
                        ulong n = read.ReadUInt64();
                        string s = read.ReadString();
                        if (s[0] != ':')
                        {
                            doc = new Document(s);
                            DocsList.Add(doc);
                        }
                        else
                            Add(n, new DocPage(doc, s.Substring(1)));

                    }
                    while (true);
                }
                catch (Exception e)
                {
                    // probably endofstream exception
                }
                // prepare for search
                _enum = this.AsEnumerable().GetEnumerator();
                _enum.MoveNext();
            }
        }
        IndexPageList PagesList;

        /* Thread Safe temporary index class */
        /* Temporary indexes then merged into final index */
        class TempIndex : SortedList<string, List<uint>>
        {
            public int nTmpIndex = 0;


            public TempIndex(string tmppath, int _MaxItems) : base()
            {
                this.MaxItems = _MaxItems;
                this.Path = tmppath;
                TotalCount = 0;
                maxCoord = 0;
            }
            public int MaxItems { get; }
            public string Path { get; }
            public int TotalCount { get; private set; }
            public ulong maxCoord { get; private set; } // maximum coordinate

            /* Add word into TemIndex
             * coord must be greater with each call */
            public void Add(string word, uint coord)
            {
                maxCoord = coord; // coord increases with each call
                List<uint> val;
                if (!base.TryGetValue(word, out val))
                {
                    val = new List<uint>();
                    base.Add(word, val);
                }
                val.Add(coord);
                TotalCount++;
                if (TotalCount > MaxItems)
                {
                    Save();
                    Clear();
                    TotalCount = 0;
                }
            }

            public void Save()
            {
                try
                {
                    Interlocked.Increment(ref nTmpIndex);
                    FileStream file = File.Create($"{Path}\\{nTmpIndex}.tmpind");
                    BinaryWriter bin = new BinaryWriter(file);
                    bin.Write(maxCoord);

                    foreach (KeyValuePair<string, List<uint>> item in this)
                    {
                        bin.Write(item.Key);
                        bin.Write(item.Value.Count);
                        // TODO: convert ot bytes
                        item.Value.ForEach((i) =>
                        {
                            bin.Write(i);
                        });
                    }

                    bin.Close();
                    //file.Close();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error saving TempIndex: " + e.Message);
                }

            }


        };

        public string WorkPath;


      
        private void IndexTask(IIndexDataSource source)
        {
           

            try
            {
                // main index task
                Directory.CreateDirectory(WorkPath + "\\" + Task.CurrentId);
                TempIndex index = new TempIndex(WorkPath + "\\" + Task.CurrentId, MaxTmpIndexItems);
                List<KeyValuePair<string, ulong>> files = new List<KeyValuePair<string, ulong>>();

                Console.WriteLine("Started IndexTask id{0}", Task.CurrentId);

                // task to process files

                
                uint coord = 0;
                do
                {
           //         Console.WriteLine($"Task {Task.CurrentId} waiting coord");
               //     coord = nextCoord.Take(cancel.Token);
             //       Console.WriteLine($"Task {Task.CurrentId} get coord {coord}");

                 //   if (cancel.Token.IsCancellationRequested) break;

                    IIndexDocument doc = source.Next();

                    if (doc == null) break;

                    Console.WriteLine("ID:{0} <-{1}", Task.CurrentId, doc.Name);
                    // TODO: File must fit RAM
                    files.Add(new KeyValuePair<string, ulong>(source.Name + ":" + doc.Name, index.maxCoord));

                    foreach (IndexPage page in doc)
                    {
                        try
                        {
                            String c = page.text.ToLower(); //File.ReadAllText(file);
                            if (c.Length == 0) continue;
                            //foreach (string ss in System.Text.RegularExpressions.Regex.Split(c, "\\b"))
                            string ss = "";
                            ss += c[0];
                            for (int qq = 1; qq < c.Length; qq++)
                            {
                                if ((qq == c.Length - 1) || (IsLetter(c[qq]) != IsLetter(c[qq - 1])))
                                {
                                    // end of group

                                    if (ss.Length > 0)
                                    {
                                        if ((ss.Length >= 2) && (IsLetter(ss[0])))
                                        {
                                            uint d = 0;
                                            string stemmed = ss;
                                            /*if (Stemm != null)
                                            {
                                                stemmed = Stemm(ss);
                                            }*/

                                            int nG = 0;
                                            int nVoc = 0;
                                            foreach (Vocab voc in vocs)
                                            {
                                                if ((voc != null) && (ss[0] >= voc.Range[0]) && (ss[0] <= voc.Range[1]) && ((stemmed = voc.Stem(ss))!=null)  && ((nG = voc.Search(stemmed)) != 0))
                                                {
                                                    // writing group number rather then parts of the word

                                                    //if (((nG & Vocab.GROUP_NOT_EXCACT_WORD_MASK) != 0) && (stemmed.Equals(ss)))
                                                    //    nG = 0;
                                                    //else
                                                    {
                                                        string str = FromInt((nVoc << 24) | (nG & Vocab.GROUP_NUMBER_MASK));
                                                        index.Add(str, coord + (uint)qq);
                                                        if ((bKeepForms) && (stemmed.Length < ss.Length))
                                                        { // reminder
                                                            str = "";
                                                            if (ss.Length - stemmed.Length <= 2)
                                                                str ="$" + ss[0] + ":" + ss.Substring(stemmed.Length);
                                                            else
                                                                str="$" + ss.Substring(stemmed.Length);
                                                            index.Add(str, coord + (uint)qq + 1);

                                                        }
                                                    }
                                                    //break;

                                                }
                                                nVoc++;
                                            }

                                            if (nG == 0)
                                            {
                                                string news = "<" + ss + ">";
                                                for (int q = 0; q < news.Length; q += SUBWORD_LENGTH)
                                                {
                                                    index.Add(news.Substring(q, Math.Min(news.Length - q, SUBWORD_LENGTH)), (coord + (uint)qq + d) / COORD_DEVIDER);
                                                    d++;
                                                }
                                            }
                                        }

                                    }
                                    ss = "";
                                }
                                ss += c[qq];
                            }
                            coord += (uint)c.Length;
                            files.Add(new KeyValuePair<string, ulong>(":" + page.id, coord));
                   //         nextCoord.Add(coord,cancel.Token);
                     //       Console.WriteLine($"Task {Task.CurrentId} pushed next coord {coord}");
                            
                        }
                        catch (Exception e)
                        { 
                            Console.WriteLine("Error parsing file {0}: {1}", doc.Name, e.Message);
                        }

                    }

                    doc.Dispose();
                }
                while (true);

                index.Save();


                FileStream fileStream = File.Create($"{WorkPath + "\\" + Task.CurrentId}\\index.tmplist");
                BinaryFormatter bin = new BinaryFormatter();
                bin.Serialize(fileStream, files);
                fileStream.Close();


            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception in Task{Task.CurrentId}: " + e.Message);
            }
            Console.WriteLine($"Task{Task.CurrentId} exited");
        }



    }
}