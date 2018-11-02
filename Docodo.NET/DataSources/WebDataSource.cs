using HtmlAgilityPack;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;


namespace Docodo
{
    public class WebDataSource : IndexTextFilesDataSource
    {
        //public string Name { get; }
        HashSet<string> urlsAdded = new HashSet<string>();
        ConcurrentQueue<string> urlsToDo = new ConcurrentQueue<string>();
        CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        //string baseurl { get; }
        private bool Navigating;
        public WebDataSource(string name,string url):base(name,url)
        {
            Name = name;
            path = new UriBuilder(url.Substring(0,url.LastIndexOf("/"))).ToString();
        }


        public override void Reset()
        {
            cancellationTokenSource.Cancel();
            cancellationTokenSource = new CancellationTokenSource();
            urlsAdded.Clear();
            urlsToDo.Clear();
            if (!Navigating)
            {
                Task.Factory.StartNew(() => { Navigating = true; Console.WriteLine($"Start web crawler from {path}"); TryAddUrl("/"); Navigate(path + "/"); Navigating = false; }, cancellationTokenSource.Token);
            }
        }

        void Navigate(string url)
        {
            var web = new HtmlWeb();
            web.UserAgent = "DOCODO";
            web.UsingCache = false;
            web.PreRequest += new HtmlWeb.PreRequestHandler((req) => {
                req.Headers.Add("accept", "text/html, text/plain");
                
                return true; });
            //web.PostResponse += new HtmlWeb.PostResponseHandler((req, resp) => {
            //});
            HtmlDocument html = web.Load(url);
            var nodes = html.DocumentNode.SelectNodes("//meta");
            if (nodes!=null)
            foreach (var node in nodes)
            {
                try
                {
                    if (node.Attributes["http-equiv"].Value.ToLower().Equals("refresh"))
                    {
                        var matches = Regex.Match(node.Attributes["content"].Value, @"url=([\w\.\\_\+\?\&]+)"); 
                        //string[] arr = node.Attributes["content"].Value.Split(';');
                        string s =TryAddUrl(matches.Groups[1].Value);
                            if (s != null)
                                Navigate(s);
                        }
                }
                catch (Exception e) { }
            }
            
            nodes = html.DocumentNode.SelectNodes("//a");
            if (nodes!=null)
            foreach (var node in nodes)
            {
                if (node.Attributes.Contains("href"))
                {
                    string s = TryAddUrl(node.Attributes["href"].Value);
                    if (s!=null)
                        Navigate(s);
                }
            }


        }

        private string TryAddUrl(string url)
        {
            string s = url.ToLower();
            if (s.Length == 0) return (null);
            if (s[0] == '#') return (null);

            if (!s.ToLower().StartsWith("http:"))
                s = path + (s.StartsWith('/')?"":"/")+ s;

            s = new UriBuilder(s).ToString();
            if (s.Length >= 4)
            {
                string ext = s.ToLower().Substring(s.Length - 4);
                if ((ext.Equals(".png")) || (ext.Equals(".svg")) || (ext.Equals(".jpg")) || (ext.Equals(".bmp")) || (ext.Equals(".gif")))
                    s = "";
            }

            if ((s.Length > 0) && (s.ToLower().StartsWith(path.ToLower())))
            {
                if (s.Length > 1024)
                    return null;

                if (!urlsAdded.Contains(s))
                {
                    urlsToDo.Enqueue(s);
                    urlsAdded.Add(s);
                    Console.WriteLine($"Parse url: {s}");
                    return (s);
                }

            }
            return (null);
        }
        override public IIndexDocument Next(bool bwait)
        {
            string str="";
            IIndexDocument ret = null;
            do
            {
                if (!urlsToDo.TryDequeue(out str))
                {
                    // Console.WriteLine($"TryDequee returns false {Navigating}, {urlsToDo.Count}");
                    if (bwait && Navigating)
                    {
                        while (Navigating)
                        {
                            if (urlsToDo.TryDequeue(out str)) break;
                            Thread.Sleep(100);
                        }
                    }

                }

                //Console.WriteLine($"TryDequee returns true {Navigating}, {urlsToDo.Count}");

                if (str == null) break; // nothing more or don't wait

                if ((str != null) && (str.Length > path.Length))
                {
                     HttpWebRequest req = HttpWebRequest.CreateHttp(str);
                     req.UserAgent = "DOCODO";
                     req.Accept = "text/html, text/plain, application/pdf";
                     req.Method = "GET";
                     WebResponse res;
                     try
                     {
                       res = req.GetResponse();
                     }
                     catch (WebException e)
                     {
                        continue;
                     }

                    /*using () */
                    {
                        if (res.ContentType.ToLower().Equals("application/pdf"))
                        {
                            ret = new DocumentsDataSource.IndexPDFDocument(str, res.GetResponseStream(), this);
                        }
                        else
                        {
                            ret = FromHtml(res.GetResponseStream(), str.Substring(path.Length),Name);
                        }
                    }
                }

            }
            while (ret == null);

           return ret;
        }

        public static IndexPagedTextFile FromHtml(Stream stream, string url,string sourcename)
        {
            HtmlDocument html = new HtmlDocument();
            html.Load(stream);
            StringBuilder builder = new StringBuilder();

            foreach (var node in html.DocumentNode.DescendantsAndSelf())
            {
                try
                {
                    if ((node.NodeType == HtmlNodeType.Text) && (!node.ParentNode.Name.Equals("script")) && (!node.ParentNode.Name.Equals("style")))
                        builder.Append(node.InnerText + " ");
                    else
                      if (node.Name.Equals("img"))
                    {
                        builder.Append(node.Attributes["alt"].Value + " ");
                    }
                }
                catch (Exception e) { }

            }
            // rectify text

            string rstr = builder.ToString().Trim(new char[] { '\r', '\n', ' ' });
            rstr = Regex.Replace(rstr, @"([ ]*[\n\r]+[ ]*)+", "\r\n");

            //                            rstr = Encoding.UTF8.GetString(Encoding.Convert(html.Encoding, Encoding.UTF8, currentText));



            if (rstr.Length > 0)
            {
                StringBuilder headers = new StringBuilder();
                string Author = "";
                string Title = "";
                var nodes = html.DocumentNode.SelectNodes("//title");
                if (nodes != null) Title = WebUtility.HtmlEncode(nodes[0].InnerText).Replace('\n', ' ').Replace('=', ' ');
                nodes = html.DocumentNode.SelectNodes("//meta");
                if (nodes != null)
                {
                    foreach (var node in nodes)
                    {
                        if (node.Attributes.Contains("Author"))
                            Author = WebUtility.HtmlEncode(node.Attributes["Author"].Value).Replace('\n', ' ').Replace('=', ' ');
                    }
                }
                headers.Append($"Name={url}\n");
                headers.Append($"Source={sourcename}\n");

                if (Title.Length > 0) headers.Append($"Title={Title}\n");
                if (Author.Length > 0) headers.Append($"Author={Author}\n");

                return new IndexPagedTextFile(url, rstr, headers.ToString());
            }
            return (null);
        }

        override public void Dispose()
        {
            cancellationTokenSource.Cancel();
            urlsAdded.Clear();
            urlsToDo.Clear();
            Navigating = false;

        }
    }

}