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
    public partial class Index
    {
        public class SearchResult
        {
            public HashSet<ResultDocument> foundDocs = new HashSet<ResultDocument>();
            public List<ResultDocPage> foundPages = new List<ResultDocPage>();
            public bool Success = true;
            public string Error { get; protected set; } = "";
            public const char BEGIN_MATCHED_SYMBOL = 'ˋ';
            public const char END_MATCHED_SYMBOL = 'ˊ';
            public override bool Equals(object obj)
            {
                if (obj.GetType() == typeof(SearchResult))
                {
                    SearchResult res = (SearchResult)obj;

                    return (res.foundPages.SequenceEqual(foundPages));
                }
                return base.Equals(obj);
            }
            public WordInfo[] words;
            public struct WordInfo
            {
                public string Word;
                public int nFound;
                public string OriginalWord;
                public int nOrigFound;
            }

        }

        public class ErrorSearchResult : SearchResult
        {
            public ErrorSearchResult(string error) : base()
            {
                Success = false;
                Error = error;
            }
        }

        /* Search result documet  class */
        public class ResultDocument : Document
        {
            public ResultDocument() : base() { }
            public ResultDocument(Document d) : base(d.Name, d.nPages) { }
            public ResultDocument(string s, int n = 0) : base(s, n) { }
            public HashSet<ResultDocPage> pages = new HashSet<ResultDocPage>();
            public Dictionary<string, string> headers;
            public float rank;
            public string summary;
            public override bool Equals(object obj)
            {
                if (obj.GetType() != GetType()) return false;

                return (((Document)obj).Name.Equals(Name));
            }
            public override int GetHashCode()
            {
                return Name.GetHashCode();
            }
            public void MakeHeaders(string str)
            {
                headers = new Dictionary<string, string>();
                string[] splits = str.Split(new char[] { '=', '\n' });
                for (int q = 0; q < splits.Length - 1; q += 2)
                {
                    headers.Add(splits[q], splits[q + 1]);
                }
            }

            public List<string> foundWords;

        }
        /* Search result documet page class */
        public class ResultDocPage
        {
            public ResultDocPage(DocPage p)
            {
                id = p.id;
            }
            public string id;
            public float rank
            {
                get
                {
                    float bonus = 0;
                    if (pos.Count > 1)
                    {
                        for (int q = 1; q < pos.Count; q++)
                            bonus += 30 / Math.Max(5, pos[q] - pos[q - 1]);
                    }
                    return 1 + bonus + (float)Math.Log(pos.Count());
                }
            }
            public List<int> pos = new List<int>(); // positions on the page
            public string text; //surrounding text
            public override bool Equals(object obj)
            {
                if (obj.GetType().Equals(typeof(ResultDocPage)))
                {
                    ResultDocPage page = (ResultDocPage)obj;
                    return (id == page.id) && (pos.SequenceEqual(page.pos));
                }
                return base.Equals(obj);
            }
        }


        protected IndexSequence SearchField(string field, string value)
        {
            try
            {
                IndexSequence seq = this[FIELD_NAME_CHAR + field.ToLower()];
                IndexSequence newseq = SearchWord(value.ToLower());
                seq.R = -1;
                return (seq * newseq);
            }
            catch (Exception e)
            {
                /* NOT FOUND */
            }

            return new IndexSequence();
            /*
                        var _matches = Regex.Matches(value, @"\b\w+\b");
                        IndexSequence orVals = new IndexSequence();
                            IndexSequence seq = this[FIELD_NAME_CHAR + field.ToLower()];
                            foreach (var match in _matches)
                            {
                                IndexSequence newseq = SearchWord(match.ToString().ToLower());
                                newseq.R = -1;
                                newseq *= seq;
                                orVals += newseq;
                            }

                        return (orVals);
                        */
        }


        public const int MAX_LIKE_WORDS = 100; // Maximum matched words when searching word pattern
        // Get list of words that match patter where _ - any letter
        public string[] GetLikeWords(string word)
        {
            if ((word.IndexOf('_') < 0) || (!bKeepForms)) return new string[] { word };
            if (word.Length < 2) return new string[] { };
            word = word.Replace("_", ".*");
            string[] s = self.Keys.Where((key) => (IsLetter(key[0]) && Regex.IsMatch(key, word))).Take(MAX_LIKE_WORDS).ToArray();
            return (s);
        }
        // Get list of word corrections if any by maximum similaraty to word using levenshtein
        public string[] GetCloseWords(string word)
        {
            List<string> result = new List<string>();
            result.AddRange(self.Keys.OrderBy((s) => s.Levenshtein(word)).Take(10).ToArray());
            return (result.ToArray());
        }

        public async Task<string[]> GetSuggessions(string req, int n = 10)
        {
            // TODO: take int acount word relations
            if (req.Length < 2) return new string[] { };
            string lastword = Regex.Split(req, @"\b").Last((s) => s.Length > 0).ToLower();

            if (lastword.Length >= 2)
            {
                return await Task.Factory.StartNew<string[]>(() => self.Where((s) => (s.Key[0] >= 'A') && (s.Key.StartsWith(lastword, StringComparison.Ordinal)) && (s.Key.Length > lastword.Length)).OrderBy((s) => -s.Value.Count).Select((s) => s.Key.Substring(lastword.Length)).Take(n).ToArray());
            }

            return new string[] { };
        }



        protected IndexSequence SearchWord(string word)
        {
            //      if (word.Length < MIN_WORD_LENGTH) return (new IndexSequence(/* Empty */));
            string stemmed = word;
            bool bExact = false;


            if ((word.ToUpper().Equals(word)) && (bKeepForms)) bExact = true;
            word = word.ToLower();

            IndexSequence total = null;

            string[] words = { word };

            if (word.IndexOf('_') >= 0)
            {
                if (bKeepForms)
                {
                    bExact = true;
                    words = GetLikeWords(word);
                }
                else
                {
                    return new IndexSequence();
                }
            }

            foreach (string wword in words)
            {

                try
                {

                    IndexSequence res = new IndexSequence();
                    var codes = GetWordCodes(wword).Select((c) => c.code);
                    if (codes.Count() > 0)
                    {

                        var selfcodes = codes.Where((s) => Regex.IsMatch(s.Substring(0, 1), @"\w"));
                        var knowcodes = codes.Except(selfcodes);
                        // search for know or exact words if any
                        foreach (string code in (!bExact ? (knowcodes.Count() > 0 ? knowcodes : selfcodes.Take(1)) : selfcodes.Take(1)))
                        {
                            if (self.Keys.Contains(code))
                            {
                                res = this[code];

                                if (total == null)
                                {
                                    total = res;
                                }
                                else
                                    total += res;

                            }
                        }
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine("Error search word: " + e.Message);
                }
            }
            if (total == null) total = new IndexSequence();

            if (bExact) total.R = -1;
            return (total);
        }

        public class SearchOptions
        {


            public int dist = 0; // max distance between words in letters
            public bool DoCorrection = false; // do correction of mistaked in words
            public bool RemoveWordBreaks = true;
            /*
            public int MaxFoundPages = MAX_FOUND_PAGES; // maximum output found pages
            public int MaxFoundDoc = MAX_FOUND_DOCS; // maximum output found docs
            public int MAX_FOUND_PAGE_TEXT = 360; // found page display text length
            const int MAX_FOUND_PAGES_IN_DOC = 1000; // maximum output found pages in one document
            */

        }


        protected delegate IndexSequence SearchFunc(string word);
        protected class SearchSequence
        {
            IndexSequence res = new IndexSequence();

            // usually lowercase, can be UPPERCASE if exact search is needed
            public string Name { get; } // variable name
            public string Word { get; set; }
            public int Cout { get => res.Count(); }
            public SearchFunc Func;
            private bool bWasCalled = false;
            public int Dist; // distance
            public SearchSequence(string name, string word, SearchFunc func, int dist = 0)
            {
                Name = name;
                Word = word;
                Func = func;
                Dist = dist;
                wordInfo.Word = Word;
                wordInfo.OriginalWord = Word;
            }

            public SearchResult.WordInfo wordInfo;
            public IndexSequence d()
            {
                if (!bWasCalled)
                {
                    res = Func(Word);
                    wordInfo.nFound = res.Count();
                    res.R = (res.R < 0 ? -Word.Length - 4 : Dist + Word.Length);
                }
                return res;

            }




        }

        protected string PrepareSearchRequest(string req, List<SearchSequence> funcs, out string fields, SearchFunc searchfunc = null, bool keepshort = false)
        {
            //List<string> fields = new List<string>();
            fields = "";


            req = Regex.Replace(req, @"[^\w(){}=~?|""]|_+", " "); // delete incorrect symbols
            string f = "";

            req = Regex.Replace(req, @"{*(\w+)[ ]*=([\w|() ]+)}", (Match match) => {
                string dummy;
                string l = PrepareSearchRequest(match.Groups[2].ToString(), funcs, out dummy, (s) => SearchField(match.Groups[1].ToString(), s), true); // like _GET("A")
                f += (f.Length > 0 ? "*" : "") + "(" + l + ")";
                return "";// $"___{fields.Count-1}";
            });

            fields = f;

            req = Regex.Replace(req, "{.*}", ""); // remove not correct fields

            //_fields = fields.ToArray();
            req = req.Replace('?', '_');


            if (!keepshort)
                req = Regex.Replace(req, @"\b\w{1,2}\b", " "); //delete words with 1,2 letters

            foreach (string st in stopWords) req = Regex.Replace(req, $@"\b{st}\b", ""); //delete stopwords
                                                                                         //req = Regex.Replace(req, @"\b(^\w|[^(+)])+\b", " ");

            req = Regex.Replace(req, "\"(.*)\"", (s) => { return "(" + s.Groups[1].Value.ToUpper() + ")"; }); // "a b" => A B

            req = Regex.Replace(req, @"\|", "+"); // replace OR operator
            // add ADD operator instead of space
            req = Regex.Replace(req, @"(\b|\))(\s+)(\b|\()", (m) => { return Regex.Replace(m.Groups[0].Value, m.Groups[2].Value, "*"); });


            //req = Regex.Replace(req, @"\b(?!___)(\w+)\b", "_Get(\"${0}\")");
            req = Regex.Replace(req, @"\b(\w+)\b", (m) => { funcs.Add(new SearchSequence("" + (char)('A' + (funcs.Count)), m.Value, (searchfunc != null) ? searchfunc : (s) => SearchWord(s))); return funcs.Last().Name + ".d()"; }); // a b => _1() _2()


            //req = req.TrimEnd(new char[] { ' ', '*' });

            return req;
        }

        protected SearchResult PrepareSearchResult(IndexSequence res, string[] filter)
        {
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
                    //if (lastResultDocPage != null)
                    //    lastResultDocPage.rank = 
                    lastResultDocPage = new ResultDocPage(_p);
                    lastResultDocPage.pos.Add((int)(coord - _p.coord));
                    result.foundPages.Add(lastResultDocPage);
                    if (prevd != _p.doc)
                    {
                        ResultDocument doc = new ResultDocument(_p.doc);
                        // check filter
                        if (result.foundDocs.Count < MAX_FOUND_DOCS)
                        {
                            bool matched = filter.Length == 0;
                            foreach (string filt in filter)
                            {
                                if (Regex.Match(doc.Name, filt).Success) { matched = true; break; }
                            }
                            if (matched)
                                result.foundDocs.Add(doc);
                        }
                        lastDoc = doc;
                    }
                    //lastResultDocPage.doc = lastDoc;

                    lastDoc.pages.Add(lastResultDocPage);
                    lastDoc.rank += lastResultDocPage.rank;
                    prevp = _p;
                    prevd = _p.doc;
                }
                else
                    lastResultDocPage.pos.Add((int)(coord - _p.coord));

                if (result.foundPages.Count > MAX_FOUND_PAGES) break;

            }
            // retrieve surrounding text

            // if (lastResultDocPage != null)
            //                lastResultDocPage.rank = 1 + (float)Math.Log(lastResultDocPage.pos.Count());



            return result;
        }

        /* Returns SearchResult var with documents that are in both res1 and res2 */
        protected SearchResult CombineSearchResults(SearchResult res1, SearchResult res2)
        {
            res1.foundDocs.IntersectWith(res2.foundDocs);

            return res1;
        }

        public async Task<SearchResult> SearchAsync(string req, SearchOptions opt = null)
        {
            return await Task.Run<SearchResult>(() =>
                Search(req, opt)
            );
        }

        public SearchResult Search(string req, SearchOptions opt = null)
        {

            //using System.Linq.Dynamic, 
            //            Expression expr = System.Linq.Dynamic.DynamicExpression.Parse(typeof(IndexSequence), "GetTest(1) * GetTest(2)", null);
            //            LambdaExpression e = Expression.Lambda(expr);
            //            IndexSequence tst = ((Func<IndexSequence>)e.Compile())();

            if (!CanSearch) return (new ErrorSearchResult("Index is not built"));
            try
            {
                lock (DoSearchLock) // wait antil can search
                {

                    var interpreter = new Interpreter();

                    req = req.ToLower();

                    /* search filter */
                    List<string> filter = new List<string>();
                    var matches = Regex.Match(req, @"\B-filter:([\w\*\?\\.()+{}/]+,?)+");
                    if (matches.Groups.Count > 1)
                    {
                        foreach (var cap in matches.Groups[1].Captures)
                        {

                            filter.Add(cap.ToString().Trim(','));
                        }
                    }
                    req = Regex.Replace(req, @"\B-filter:([\w\*\?\\.()+{}/]+,?)+", " ");

                    string fields;
                    List<SearchSequence> seq = new List<SearchSequence>();
                    req = PrepareSearchRequest(req, seq, out fields);

                    int R = 255;
                    if (opt != null) R = opt.dist;
                    bool DoCorrection = (opt != null) ? opt.DoCorrection : true;



                    /* 
                                        Func<string, IndexSequence> Get = (word) => { IndexSequence seq = SearchWord(word); seq.R = Math.Sign(R) * (word.Length + Math.Abs(R)); return (seq); };
                                        Func<string, IndexSequence> GetField = (namevalue) => {
                                            // Search by field name=value using OR combinatin operator for value subwords
                                            string name = namevalue.Split('=')[0];
                                            string value = namevalue.Split('=')[1];
                                            return (SearchField(name, value));
                                        };
                                        interpreter.SetFunction("_Get", Get);
                                        interpreter.SetFunction("_Getf", GetField);
                        */



                    seq.Select((s, i) => new { name = s.Name, value = s }).All((o) => { interpreter.SetVariable(o.name, o.value); o.value.Dist = R; return true; });


                    IndexSequence res = null;
                    IndexSequence resf = null;
                    if (req.Length > 0)
                    {
                        try
                        {
                            res = interpreter.Eval<IndexSequence>(req);
                        }
                        catch (DynamicExpresso.Exceptions.DynamicExpressoException e)
                        {
                            string s = "Syntax Error in search request";
                            Console.WriteLine(s);
                            return new ErrorSearchResult(s);
                        }
                    }

                    /* try to improve result
                    int nLowResult =  Math.Max(3,(int)(MaxCoord/100000));
                    int nLowWordResut = Math.Max(10,(int)(MaxCoord/10000));
                    if ((res.Count<nLowResult) && (DoCorrection)) 
                    {
                        // max 3 corrected words
                        SearchSequence[] low = seq.Where((z)=>(z.Word.ToLower().Equals(z.Word)) && (z.Word.IndexOf('_')<0)).OrderBy((z)=>z.wordInfo.nFound).Where((z)=>z.wordInfo.nFound<nLowWordResut).Take(3).ToArray();
                        List<string>[] replaces = new List<string>[low.Length];

                        low[0].Word = 

                    }
*/

                    if (fields.Length > 0)
                    {
                        try
                        {
                            resf = interpreter.Eval<IndexSequence>(fields);
                        }
                        catch (DynamicExpresso.Exceptions.DynamicExpressoException e)
                        {
                            string s = "Syntax Error in search request";
                            Console.WriteLine(s);
                            return new ErrorSearchResult(s);
                        }
                    }
                    if (res == null) res = resf;
                    if (res != null)
                    {


                        SearchResult result = PrepareSearchResult(res, filter.ToArray());
                        if (resf != null)
                        {
                            result = CombineSearchResults(result, PrepareSearchResult(resf, new string[] { }));
                        }




                        foreach (var doc in result.foundDocs)
                        {
                            doc.rank = doc.pages.Sum((page) => { return page.rank; });
                            doc.rank = 1 + (float)Math.Log(doc.rank);
                            if (doc.pages.First().id.Equals("0"))
                                doc.rank *= DOC_RANK_MULTIPLY;
                            doc.foundWords = new List<string>();

                            foreach (var source in sources)
                                if (source.Name.Equals(doc.Name.Split(':')[0]))
                                {
                                    if ((source != null) && (source is IIndexDirectDataSource))
                                    {
                                        IIndexDirectDocument document = (source as IIndexDirectDataSource)[doc.Name.Substring(doc.Name.Split(':')[0].Length + 1)];
                                        if (document != null)
                                        {
                                            string headers = document["0"].text;
                                            if (doc.pages.First().id.Equals("0"))
                                            {
                                                SpannableString sp = new SpannableString.Builder().Add(headers, doc.pages.First().pos.ToArray());
                                                //doc.foundWords.AddRange(sp.Where(s => s.format != 0).Select(s => s.text).Distinct().ToList());
                                                headers = sp;
                                            }
                                            doc.MakeHeaders(headers);
                                            doc.pages.RemoveWhere((p) => { return p.id.Equals("0"); });
                                            foreach (var page in doc.pages)
                                            {
                                                SpannableString str = PreparePageText(page, document[page.id].text,MAX_FOUND_PAGE_TEXT);
                                                doc.foundWords.AddRange(str.Where(s => s.format != 0).Select(s => s.text).Distinct().ToList());
                                                page.text = str;
                                                //page.text.Replace("\r\r", " ");
                                                //page.text = page.text.Replace("\n\n", " ");

                                            }
                                            if (doc.pages.Count > 0)
                                            {
                                                doc.summary = doc.pages.OrderBy(pg => pg.rank).Take(3).OrderBy(pg => pg.id).Select((pg) => pg.text).Aggregate((a, b) => a + " ... " + b);
                                            }

                                            document.Dispose();
                                        }
                                    }
                                    break;
                                }
                            doc.foundWords = doc.foundWords.Distinct().ToList();
                        }

                        result.foundDocs = new HashSet<ResultDocument>((from doc in result.foundDocs orderby doc.rank select doc).ToArray());

                        result.words = seq.Select((i) => i.wordInfo).ToArray();

                        return result;

                    }
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
                return new ErrorSearchResult("Error: " + e.Message);
            }

            Console.WriteLine("Not fond!");
            return (new SearchResult());
        }

        internal class StringSpan
        {
            public int format;
            public string text;
        }

        internal class SpannableString : LinkedList<StringSpan>
        {
            public SpannableString Substring(int start, int len)
            {
                Builder res = new Builder();

                int l = 0;
                LinkedListNode<StringSpan> sp = First;

                do
                {
                    l += sp.Value.text.Length;
                    if ((res.Count == 0) && (l > start))
                    {
                        // start 
                        if (sp.Value.format != 0)
                        {
                            res.Add(sp.Value);
                        }
                        else
                            res.Add(sp.Value.text.Substring(start - l + sp.Value.text.Length), 0);
                    }
                    else
                    if (res.Count > 0)
                    {
                        if (l >= start + len)
                        {
                            // finish
                            if (sp.Value.format != 0)
                            {
                                res.Add(sp.Value);
                            }
                            else
                            {
                                res.Add(sp.Value.text.Substring(0, start + len - l + sp.Value.text.Length), 0);
                            }
                            break;
                        }
                        res.Add(sp.Value);
                    }


                } while ((sp = sp.Next) != null);

                return res;
            }

            public SpannableString RegexReplace(string pattern, string replace)
            {
                foreach (var sp in this)
                {
                    sp.text = Regex.Replace(sp.text, pattern, replace);
                }
                return this;
            }

            public static implicit operator string(SpannableString st)
            {
                string res = "";
                foreach (StringSpan sp in st)
                {
                    sp.text.Replace(SearchResult.BEGIN_MATCHED_SYMBOL, '\'');
                    sp.text.Replace(SearchResult.END_MATCHED_SYMBOL, '\'');
                    if (sp.format != 0) res += $"{SearchResult.BEGIN_MATCHED_SYMBOL}{sp.text}{SearchResult.END_MATCHED_SYMBOL}";
                    else res += sp.text;
                }
                return res;
            }

            public class Builder
            {
                SpannableString res = new SpannableString();
                public static implicit operator SpannableString(Builder b) { return b.res; }
                public static implicit operator string(Builder b) { return b.res; }
                public Builder Add(string _text, int _format)
                {
                    res.AddLast(new StringSpan() { format = _format, text = _text });
                    return this;
                }

                public Builder Add(string text, int[] startwords)
                {
                    int lastpos = 0;
                    foreach (var pos in startwords)
                    {
                        Add(text.Substring(lastpos, pos - lastpos), 0);
                        int wordend = Regex.Match(text.Substring(pos), @"(?<=\w)\b").Index;
                        Add(text.Substring(pos, wordend), 1);
                        lastpos = pos + wordend;
                    }
                    Add(text.Substring(lastpos), 0);
                    return this;
                }

                public Builder Add(StringSpan sp)
                {
                    res.AddLast(sp);
                    return this;
                }

                public int Count { get => res.Count; }

            }
        }


        private static SpannableString PreparePageText(ResultDocPage page, string text,int Len)
        {
            // first define spanns
            SpannableString.Builder spans = new SpannableString.Builder();
            spans.Add(text, page.pos.ToArray());

            int[] Range = { 0, 0 };
            Range[0] = Math.Min(Math.Max(0, page.pos.Min() - Len / 4), text.Length);
            Range[1] = Math.Min(Math.Min(page.pos.Max() + Len / 4, text.Length), Range[0] + Len);

            SpannableString res = ((SpannableString)spans).Substring(Range[0], Range[1] - Range[0]);

            res = res.RegexReplace(@"\b\W*\.+\W*\b", ". ");
            res = res.RegexReplace(@"\b\W*\?+\W*\b", "? ");
            res = res.RegexReplace(@"\b\W*!+\W*\b", "! ");
            res = res.RegexReplace(@"\b\W*:+\W*\b", ": ");
            res = res.RegexReplace(@"\b\W*,+\W*\b", ", ");

            return (res);

        }

    }
}