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
using System.Linq;

namespace Docodo
{
    public class WebDataSource : QueuedDataSource<string>
    {
        //public string Name { get; }
        protected bool GoAway = true;  // Go outside initial path
        HashSet<string> urlsAdded = new HashSet<string>();

        //string baseurl { get; }
        private string host;
        private string indextypes = "";
        public WebDataSource(string name, string url, string indextypes="") : base(name, url)
        {
            Name = name;
            if (!url.EndsWith("/")) url += "/";
            Path = new UriBuilder(url).ToString().ToLower();
            host = new UriBuilder(Path).Host;
            //this.indextypes = "."+indextypes.Split(',').Aggregate((a, b) => a + "," + "."+b);
            this.indextypes = indextypes;
        }

        protected override void Navigate(ConcurrentQueue<string> queue, CancellationToken token)
        {
            Navigate(queue, token, Path);
        }


        protected void Navigate(ConcurrentQueue<string> queue, CancellationToken token,string url)
        {
            var web = new HtmlWeb();
            web.UserAgent = "DOCODO";
            web.UsingCache = false;
            web.PreRequest += new HtmlWeb.PreRequestHandler((req) => {
                req.Headers.Add("accept", "text/html, text/plain");

                return true; });
            //web.PostResponse += new HtmlWeb.PostResponseHandler((req, resp) => {
            //});
            Console.WriteLine($"Parse url: {url}");


            HtmlDocument html = web.Load(url);
            var nodes = html.DocumentNode.SelectNodes("//meta");
            if (nodes != null)
                foreach (var node in nodes)
                {
                    try
                    {
                        
                        if ((node.Attributes.Contains("http-equiv")) && (node.Attributes["http-equiv"].Value.ToLower().Equals("refresh")))
                        {
                            var matches = Regex.Match(node.Attributes["content"].Value, @"url=([\w\.\\_\+\?\&]+)");
                            //string[] arr = node.Attributes["content"].Value.Split(';');
                            string s = TryAddUrl(queue,matches.Groups[1].Value);
                            if (s != null)
                                Task.Run(() => Navigate(queue,token,s));
                        }
                    }
                    catch (Exception e) {
                    }
                    token.ThrowIfCancellationRequested();
                }

            nodes = html.DocumentNode.SelectNodes("//a");
            if (nodes != null)
                foreach (var node in nodes)
                {
                    if (node.Attributes.Contains("href"))
                    {
                        string s = TryAddUrl(queue,node.Attributes["href"].Value);
                        if (s != null)
                        {
                            Navigate(queue,token,s);
                            Thread.Sleep(100);
                        }
                    }
                    token.ThrowIfCancellationRequested();
                }


        }

        int Count = 0;
        public int MaxItems = 1000000;

        private string TryAddUrl(ConcurrentQueue<string> queue,string url)
        {
            string s = url.ToLower();
            if (s.Length == 0) return (null); 
            if (s[0] == '#') return (null);
            if ((!s.Contains("://")) && (!s.Contains(":\\")))
                {
                s = this.Path + s;
            }

            try
            {
                s = new UriBuilder(s).ToString();
            }
            catch (UriFormatException e)
            {
                return null;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error parsing url: " + url);
                return null;
            }
            string _host = new UriBuilder(s).Host;

            string ext = "";
            if (s.Length >= 4)
            {
                if (s.LastIndexOf('.')!=-1) 
                 ext = s.Substring(s.LastIndexOf('.'));
                if (ext.LastIndexOf('?') >= 0) // remove query string
                    ext = ext.Substring(0, ext.LastIndexOf('?'));

                if ((ext.Equals(".png")) || (ext.Equals(".svg")) || (ext.Equals(".jpg")) || (ext.Equals(".bmp")) || (ext.Equals(".gif")))
                    s = "";
            }

            if ((s.Length > 0) && /*((s.StartsWith(path))

                || */ (host.Equals(_host)) /*&& (ext.Equals(".pdf"))) // pdf's can be somewhere on host */

                )
            {
                if (s.Length > 1024)
                    return null;

                if (!urlsAdded.Contains(s))
                {
                    //if (ext.Length == 0) ext = ".html";
                    //if (!ext.Equals(".pdf")) ext = ".html";
                    if ((indextypes.Length == 0) || (Regex.IsMatch(s, indextypes)))
                    {
                        if (Count < MaxItems)
                        {
                            Enqueue(queue,s);
                            Count++;
                        }
                    }
                    urlsAdded.Add(s);
                    
                    return (s);
                }

            }
            return (null);
        }

        protected override IIndexDocument DocumentFromItem(string item)
        {
            return FromUrl(item, this);
        }

        /* Create IIndexDocument instance parsing url using parent as parent of created instance,
         * returns one of the known documents: html,txt,pdf, ... depending on server url responce content-Type*/
        public static IIndexDocument FromUrl(string url,IIndexDataSource parent)
        {
            HttpWebRequest req = HttpWebRequest.CreateHttp(url);
            req.UserAgent = "DOCODO";
            req.Accept = "text/html, text/plain, application/pdf";
            req.Method = "GET";
            IIndexDocument ret = null;
            WebResponse res;
            try
            {
                res = req.GetResponse();
            }
            catch (WebException e)
            {
                return null;
            }
            
            if (res.ContentType.ToLower().Equals("application/pdf"))
            {
                ret = new DocumentsDataSource.IndexPDFDocument(url, res.GetResponseStream(), parent);
            }
            else
            if (res.ContentType.ToLower().Equals("text/plain"))
            {
                using (StreamReader reader = new StreamReader(res.GetResponseStream()))
                {
                    ret = new IndexPagedTextFile(url.Substring(parent.Path.Length), reader.ReadToEnd(), "Source=" + parent.Name);
                }
            }
            else
            {
                ret = FromHtml(res.GetResponseStream(), url.Substring(parent.Path.Length), parent.Name);
            }


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
                        if (node.Attributes.Contains("alt"))
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

    }

}