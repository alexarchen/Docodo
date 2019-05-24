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

// Chagelog
// static Variables 
// SearchAsync

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

static class Utils
{
    /// <summary>
    /// Compute the distance between two strings.
    /// </summary>
    public static int Levenshtein(this string s, string t)
    {
        int n = s.Length;
        int m = t.Length;
        int[,] d = new int[n + 1, m + 1];

        // Step 1
        if (n == 0)
        {
            return m;
        }

        if (m == 0)
        {
            return n;
        }

        // Step 2
        for (int i = 0; i <= n; d[i, 0] = i++)
        {
        }

        for (int j = 0; j <= m; d[0, j] = j++)
        {
        }

        // Step 3
        for (int i = 1; i <= n; i++)
        {
            //Step 4
            for (int j = 1; j <= m; j++)
            {
                // Step 5
                int cost = (t[j - 1] == s[i - 1]) ? 0 : 1;

                // Step 6
                d[i, j] = Math.Min(
                    Math.Min(d[i - 1, j] + 1, d[i, j - 1] + 1),
                    d[i - 1, j - 1] + cost);
            }
        }
        // Step 7
        return d[n, m];
    }

   }


    public partial class Index : IEnumerable<KeyValuePair<string,IndexSequence>>, IDisposable
    {
        public int MAX_DEF_TMP_INDEXITEMS = 1000001; // Maximum items in tempindex
        public static int MAX_WORD_LENGTH = 32;      // Maximum word length
        const long MAX_FILE_SIZE = 200000000; // Maximum indexable text file size
        const int COORD_DEVIDER = 1; // devider of coordinates in index
        const int SUBWORD_LENGTH = 4; // words in index are splittered by groups of this number of chars
        public int MAX_FOUND_PAGES = 30000; // maximum output found pages
        public int MAX_FOUND_DOCS = 500; // maximum output found docs
        public int MAX_FOUND_PAGE_TEXT = 320; // found page display text length
        const int MAX_FOUND_PAGES_IN_DOC = 1000; // maximum output found pages in one document
        const char WORD_SUFFIX_CHAR = '$'; // char prefix of word stemming remainder
        const char WORD_STEM_CHAR = WORD_SUFFIX_CHAR; // char prefix of word stemming remainder
        const char SUFFIX_DEVIDER_CHAR = ':';
        const char DOC_SEP = ':'; // document name from source name separator in pageslist
        const char WORD_BEGIN_CHAR = '<';
        const char WORD_END_CHAR = '>';
        const char KNOWN_WORD_CHAR = '#'; // char prefix to word nG from vocab
        const char FIELD_NAME_CHAR = '&'; // char prefix to field name 
        public int MIN_WORD_LENGTH = 3;
        public static string DEFAULT_PATH = ".\\index\\"; // default path to store index files
        public static float DOC_RANK_MULTIPLY = 10; // Rank multiplier when found in headers


        /* Index constructor
         *   path - working folder, if set it will load index
         *            autmatically, else set WorkPath before call to Load or CreateAsync
         *   InMemory - if true load into memory, else on disk         
         *   vocs - vocabs to use with index 
         */
        public Index(string path=null, bool InMemory=false, Vocab []vocs=null)
        {
          
            MaxDegreeOfParallelism = 2;
            MaxTmpIndexItems = MAX_DEF_TMP_INDEXITEMS;
            if (path != null)
                WorkPath = path;
            else WorkPath = DEFAULT_PATH;

            if (path !=null)
                Load();
            
            this.InMemory = InMemory;

            if (vocs!=null)
             this.vocs = new List<Vocab>(vocs);
        }

    public class IndexComparer: IComparer<string>
    {
        public int Compare (string a,string b){

         return String.CompareOrdinal(a,b);
        }

    }

        SortedList<string, IndexSequence> self = new SortedList<string, IndexSequence>(new IndexComparer());

        public bool InMemory { get; private set; } = false; /* Load into memory or leave on disk */
        public bool bKeepForms { get; set; } = true;/* Keep full forms of the words */
        public bool CanSearch { get; private set; } = false; /* If index is loaded */

        // ConcurrentStemmer class for multithread stemmer using
        public class ConcurrentStemmer : IStemmer
        {
            IStemmer _base;
            public ConcurrentStemmer (Type type)
            {
                this._base = (IStemmer) type.GetConstructor(new Type[]{ }).Invoke(new object[] { });
            }
            object stemlock = new object();
            public string Stem(string word)
            {
                lock (stemlock)
                {
                    return _base.Stem(word);
                }
            }
        }
        ///  Stemmers table for string name
        public static (string lang, IStemmer stemmer, string range)[] KnownStemmers = {
                ("digit",null,"0-9"),
                ("ru",new ConcurrentStemmer(typeof(RussianStemmer)),"а-яё"),
                ("en",new ConcurrentStemmer(typeof(EnglishStemmer)),"a-z"),
                ("de",new ConcurrentStemmer(typeof(GermanStemmer)),"a-zẞäüö"),
                ("fr",new ConcurrentStemmer(typeof(FrenchStemmer)),"a-zéâàêèëçîïôûùüÿ")
            };

        public List<(string lang, IStemmer stemmer, string range)> Stemmers = new List<(string lang, IStemmer stemmer, string range)>(KnownStemmers);
        /*
        public static (string lang, Type type)[] Stemmers = {
                ("ru",typeof (RussianStemmer)),
                ("en",typeof (EnglishStemmer)),
                ("de",typeof (GermanStemmer)),
                ("fr",typeof (FrenchStemmer)),
                ("sp",typeof (SpanishStemmer)),
                ("it",typeof (ItalianStemmer))
            };*/



        private static string FromInt(int i) { return (KNOWN_WORD_CHAR + String.Format("{0:X}", i)); } // Convert.ToBase64String(new byte[] { (byte)((i >> 24) & 0xFF), (byte)((i >> 16) & 0xFF), (byte)((i >> 8) & 0xFF), (byte)((i) & 0xFF) }).TrimEnd('=')); }

        private IndexSequence LoadSequence(IndexSequence seq)
        {
            if (reader != null)
            {
                IEnumerator<ulong> e = seq.GetEnumerator();
                e.MoveNext();
                int len = (int) e.Current;
                e.MoveNext();
                long off = (long) e.Current - len;
                reader.BaseStream.Seek(off, SeekOrigin.Begin);
                byte[] buffer = new byte[len * 2];
                reader.BaseStream.Read(buffer, 0, len * 2);
                ushort[] arr = new ushort[len];
                Buffer.BlockCopy(buffer, 0, arr, 0, len*2);
                return (new IndexSequence(arr));

            }
            return (null);
        }

        private StreamReader reader = null;
        protected List<Vocab> vocs = new List<Vocab>();

        public void AddVoc (Vocab voc)
        {
            vocs.Add(voc);
        }

        public HashSet<string> stopWords = new HashSet<string>();
        public void LoadStopWords(string file)
        {
            stopWords = new HashSet<string>((from string s in File.ReadAllLines(file) where ((s.Trim(' ').Length > 0) && (!s.Contains(';'))) select s).ToList());
        }
        public void AddStopWords(string [] words)
        {
            foreach (string w in words) stopWords.Add(w);

        }

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

        public int Count { get => self.Count; }

        public IndexSequence this[string key]
        {
           get {
            IndexSequence seq = self[key];
                if (!InMemory)
                    seq = LoadSequence(seq);
            return (seq);
            }
        }
        public IEnumerator<KeyValuePair<string, IndexSequence>> GetEnumerator() { return self.GetEnumerator(); }

        IEnumerator IEnumerable.GetEnumerator()=>GetEnumerator();


        // returns words list by string code
        public string GetWordsGroup(string  code)
        {
            if (code[0] == KNOWN_WORD_CHAR) code = code.Substring(1);

            return (GetWordsGroup(int.Parse(code, System.Globalization.NumberStyles.AllowHexSpecifier)));
        }
        // returns words list by voc group number
        public string GetWordsGroup(int nG)
        {
            int nV = (nG >> 24);
            return (from ii in vocs[nV] where (ii.Value == (nG & 0xFFFFFF)) select ii).Take(20).Select((i) => i.Key).Aggregate((a, b) => { return (a + "," + b); });
        }

        // get most popular words
        public static Dictionary<string, int> CalcHistogram(Index index)
        {
            Dictionary<string, int> dict = new Dictionary<string, int>();
            try
            {
                foreach (var pair in (from item in index let v = (index.InMemory ? item.Value.Count : (int)item.Value.First()) orderby -v select new KeyValuePair<string, int>(item.Key, (int)v)).Take(1000))
                {
                    if (pair.Key[0] == KNOWN_WORD_CHAR)
                    {
                        
                        
                        dict.Add("("+index.GetWordsGroup(pair.Key.Substring(1)) +")",
                            pair.Value);
                    }
                    else
                    dict.Add(pair.Key, pair.Value);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in Histogram: " + e.Message);
            }
            return (dict);
        }

        // maximum word coordinate in index, equals total length of text
        public ulong MaxCoord { get; private set; } = 0;

        public bool Load()
        {
            if ((File.Exists(Path.Combine(WorkPath,".index"))) && (File.Exists(Path.Combine(WorkPath,".index.list"))))
            {
                CanSearch = false;
                self.Clear();

                try
                {
                    BinaryReader bin = new BinaryReader(File.OpenRead(Path.Combine(WorkPath,".index")));


                    try
                    {
                        MaxCoord = bin.ReadUInt64();
                        do
                        {
                            string s = bin.ReadString();
                                          
                            //if (n > 0)
                            //{
                                if (InMemory)
                                {

                                    /*ushort[] arr = new ushort[n];
                                    byte[] bytes = bin.ReadBytes(n * sizeof(int));
                                    Buffer.BlockCopy(bytes, 0, arr, 0, n * sizeof(ushort));
                                    */
                                    IndexSequence seq = new IndexSequence();
                                    seq.Read(bin);
                                    self.Add(s, seq);
                                }
                                else
                                {
                                    int n = bin.ReadInt32();
                                    self.Add(s, new IndexSequence.Builder().Add((ulong)n).Add((ulong)(bin.BaseStream.Position+n)).build());
                                    bin.BaseStream.Seek(n * IndexSequence.DataUnitSize, SeekOrigin.Current);
                                }
                            //}
                            //else throw new InvalidDataException();

                        }
                        while (true);
                    }
                    catch (EndOfStreamException e)
                    {

                    }

                    if (!InMemory) { reader = new StreamReader(bin.BaseStream); }
                    else bin.Close();

                    bin = new BinaryReader(File.OpenRead(Path.Combine(WorkPath,".index.list")));

                    PagesList = new IndexPageList();
                    PagesList.Load(bin);
                    bin.Close();

                    CanSearch = true;
                    return (true);

                }
                catch (Exception e)
                {
                  Console.WriteLine("Can't load: "+e.Message);
                }
            }
            return (false);
        }

        public void Cancel()
        {
            cancelationToken.Cancel();
        }


        private ParallelOptions po = new ParallelOptions();
        public CancellationTokenSource cancelationToken { get; private set; }
        public int MaxDegreeOfParallelism { get => po.MaxDegreeOfParallelism; set => po.MaxDegreeOfParallelism = value; }

        public enum Status
        {
            Idle, Nav, Index, Merge
        };

        public Status status { get; private set; }
        public bool IsCreating { get => status != Status.Idle; }
        private object DoSearchLock = new object();

        private IIndexDataSource[] sources;
        public int nDataSources { get => sources!=null?sources.Length:0; }
        public bool CanIndex { get => (nDataSources > 0) && (!IsCreating); }

        const string CACHE_END = ".cache.zip";
        public void AddDataSource(IIndexDataSource source)
        {
            source = new IndexTextCacheDataSource(source, Path.Combine(WorkPath,source.Name + CACHE_END));
            if (sources == null) { sources = new IIndexDataSource[1]; sources[0] = source; }
            else
            {
             Array.Resize(ref sources, sources.Length + 1);
             sources[sources.Length - 1] = source;
            }
            
        }



        event EventHandler CreationDone;


      
        public async Task CreateAsync()
        {
            if ((sources == null) || (sources.Length == 0) || (sources[0] == null)) { if (CreationDone!=null) CreationDone(this,new EventArgs()); return; }// Task.FromException(new Exception("No data sources")); }

            if (status == Status.Idle)
            { //first time
                //Close();
                long startTime = Environment.TickCount;

                status = Status.Nav;
                cancelationToken  = new CancellationTokenSource();
                po.CancellationToken = cancelationToken.Token;

                await (Task.Run(async () =>
                {
                try
                {

                    if (Directory.Exists(WorkPath))
                        foreach (string dir in Directory.GetDirectories(WorkPath))
                            Directory.Delete(dir, true);
                    else
                        Directory.CreateDirectory(WorkPath);


                    List<Task> tasks = new List<Task>();


                        List<IIndexDataSource> tmpSources = new List<IIndexDataSource>();
                    foreach (IIndexDataSource source in sources)
                    {
                        IIndexDataSource tmpsource = source;
                        if (source.GetType().Equals(typeof(IndexTextCacheDataSource)))
                        {
                            tmpsource = new IndexTextCacheDataSource(((IndexTextCacheDataSource)source).source, Path.Combine(WorkPath,source.Name + CACHE_END + "_"));
                            tmpSources.Add(tmpsource);

                        }

                        tmpsource.Reset();
                        for (int q = 0; q < Math.Max(1, MaxDegreeOfParallelism); q++)
                            tasks.Add(Task.Factory.StartNew(() => { IndexTask(tmpsource);}, po.CancellationToken));
                    }
                    
                    status = Status.Index;
                    await Task.WhenAll(tasks.ToArray());//, po.CancellationToken);
                    Console.WriteLine("Index finished");

                    // dispose temporaty sources
                    foreach (IIndexDataSource source in tmpSources)
                       source.Dispose();

                    // Next parallel Merge 
                    status = Status.Merge;
                    Directory.GetDirectories(WorkPath).AsParallel().ForAll((i) => MergeAll(i));
                    // Overall Merge
                        Console.WriteLine("Final merge...");
                    ArrayList files = new ArrayList();
                    foreach (string dir in Directory.GetDirectories(WorkPath))
                    {
                        string[] f = Directory.GetFiles(dir, "*.tmpind");
                        if (f.Length > 0) files.Add(f[0]);
                    }

                    MergeIndexes((String[])files.ToArray(typeof(string)), Path.Combine(WorkPath,".index"));
         
                     //CanSearch now false   

                        // exchanging datasources
                        lock (DoSearchLock)
                        {
                            List<IIndexDataSource> ds = new List<IIndexDataSource>();
                            foreach (IIndexDataSource source in sources)
                            {
                                if (source.GetType().Equals(typeof(IndexTextCacheDataSource)))
                                {
                                    source.Dispose(); // free resources
                                    File.Delete(Path.Combine(WorkPath,source.Name + CACHE_END));
                                    File.Move(Path.Combine(WorkPath,source.Name + CACHE_END + "_"), Path.Combine(WorkPath ,source.Name + CACHE_END));
                                    ds.Add(new IndexTextCacheDataSource(((IndexTextCacheDataSource)source).source, Path.Combine(WorkPath , source.Name + CACHE_END)));
                                }
                                else ds.Add(source);


                            }
                            sources = ds.ToArray();
                        }

                        Load();
                        CanSearch = true;

                        Array.ForEach(Directory.GetDirectories(WorkPath), i => Directory.Delete(i, true));

                        status = Status.Idle;

                        Console.WriteLine("Time elasped: {0} s", (Environment.TickCount - startTime) / 1000);
                    }
                    catch (Exception e)
                    {
                      Console.WriteLine("Error: "+e.Message);
                      status = Status.Idle;
                      CanSearch = false;
                       
                    }
                    cancelationToken = null;
                }, cancelationToken.Token));

            }
 
           if (CreationDone!=null) CreationDone(this,new EventArgs());
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
                IndexSequence[] arr = new IndexSequence[files.Length]; // coord arrays

                for (int q = 0; q < s.Length; q++) {
                    s[q] = " ";
                    arr[q] = new IndexSequence();
                }
                
                //int[] n = new int[files.Length]; // numbers of coords in vectors
                bool[] readnext = new bool[files.Length]; // what index read next
                //Array.Fill(readnext, true); // need to read from each index first
                for (int q = 0; q < readnext.Length; q++) readnext[q] = true;

                var s_end = from str in s where str.Length != 0 select str;
                
                do
                {
                    for (int q = 0; q < files.Length; q++)
                        if ((readnext[q]) && (s[q].Length > 0))// read next string & array if previous was read
                        {
                            try
                            {
                                s[q] = bins[q].ReadString();
                                arr[q].Read(bins[q]);

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
                    int i = String.CompareOrdinal(a.Key,b.Key);
                        if (i == 0)
                        {
                            if (shift_coords) return (maxCoords[a.Value].CompareTo(maxCoords[b.Value]));
                            else return (shifts[a.Value].CompareTo(shifts[b.Value]));
                        }
                        return (i);
                    }));
                    //Array.Fill(readnext, false); // .Net Core method
                    for (int q = 0; q < readnext.Length; q++)
                        readnext[q] = false;

                    if (list.Count > 0)
                    {
                        readnext[list[0].Value] = true;
                        //int nsize = n[list[0].Value];

                        for (int q = 1; q < list.Count; q++)
                            if (list[q].Key.Equals(list[q - 1].Key))
                            {
                                readnext[list[q].Value] = true;
                                //nsize += n[list[q].Value];
                            }

                        wr.Write(s[list[0].Value]);
                        //wr.Write(nsize);
                        IndexSequence.Builder bldr = new IndexSequence.Builder();
                        for (int q = 0; q < files.Length; q++)
                            if (readnext[q])
                            {
                                // write coord array
                          
                                if (shift_coords) { arr[q].Shift(shifts[q]); } //for (int w = 0; w < arr[q].Length; w++) arr[q][w] += (uint)(shifts[q]); }
                                bldr.AddRange(arr[q]);
                            }
                        bldr.build().Write(wr);
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
        void MergeIndexes(string[] files, string output)
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
                    FileStream f = File.OpenRead(Path.Combine(new FileInfo(files[q]).DirectoryName, "index.tmplist"));

                    PagesList.AddFromList((List<KeyValuePair<string, ulong>>)binf.Deserialize(f),shifts[q]);

                    f.Close();

                }

                lock (DoSearchLock) // stop all seach tasks before replacing index
                {
                    CanSearch = false;
                }
                    if (File.Exists(output + ".list")) File.Delete(output + ".list");
                    BinaryWriter binOut = new BinaryWriter(File.Create(output + ".list"));
                    PagesList.Save(binOut);
                    binOut.Close();

                    
                    Close();
                    if (File.Exists(output)) File.Delete(output);
                    File.Move(output + "_", output);
                


                foreach (string file in files) File.Delete(file);


            }
            catch (Exception e)
            {
                Console.WriteLine("Error mering indexes: " + e.Message);
            }

            if (File.Exists(output + "_"))
             File.Delete(output+"_");
        }

       

        protected virtual bool IsLetter(char l) 
         {   
//             if (l == '_') return (false);
             return Regex.IsMatch(""+l,@"\p{L}");
         }
        protected virtual bool IsLetterOrDigit(char l)
        {
            //             if (l == '_') return (false);
            return Regex.IsMatch("" + l, @"\w");
        }


        /* Call to free all resources used by index */
        public void Dispose()
        {
            CanSearch = false;
            if (sources != null)
            {
                foreach (IIndexDataSource source in sources)
                    if (source.GetType() == typeof(IndexTextCacheDataSource))
                    {
                        source.Dispose();
                    }
            }
            sources = new IIndexDataSource[] { };
            if (PagesList!=null)
             PagesList.Clear();
            self.Clear();
            if (reader!=null)
             reader.Dispose();
        }
    }
}