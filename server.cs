using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace Docodo
{
    public class DocodoServer
    {
        int MAX_SERVER_THREADS = Environment.ProcessorCount * 4;
        TcpListener Listener;
        
        public DocodoServer(int nPort)
        {
            Listener = new TcpListener(IPAddress.Any, nPort);
            Listener.Start();
            Console.WriteLine($"Http server listening on port {nPort}...");
            Task.Factory.StartNew(() =>
            {

                while (Listener != null)
                {
                    ThreadPool.SetMaxThreads(MAX_SERVER_THREADS,MAX_SERVER_THREADS);
                    ThreadPool.QueueUserWorkItem(new WaitCallback(ClientThread), Listener.AcceptTcpClient());
                    
                }

            });

        }

        private void ClientThread(object state)
        {
            new Client((TcpClient)state);
        }

        ~DocodoServer()
        {

            if (Listener != null)
            {

                Listener.Stop();
            }
        }

        class Client 
        {
            TcpClient client;

            string GetError(int Code)
            {
               return ("HTTP/1.1 " + Code.ToString() + " " + ((HttpStatusCode)Code).ToString() + "\n\n" + Code.ToString() + " " + ((HttpStatusCode)Code).ToString());
            }
            public Client(TcpClient tcpClient)
            {
                client = tcpClient;

                string Request = "";
                byte[] Buffer = new byte[1024];
                
                int Count;

                string Resp = "HTTP / 1.1 200 OK\nContent-type:text/html; charset=utf-8\n\n";

                while ((Count = client.GetStream().Read(Buffer, 0, Buffer.Length)) > 0)
                {
                    Request += Encoding.ASCII.GetString(Buffer, 0, Count);
                    if (Request.IndexOf("\r\n\r\n") >= 0) break;
                }
                if (Request.StartsWith("GET /search?req="))
                {
                    var matches = Regex.Match(Request, @"search\?req=([^ &]+)");
                    try
                    {
                        string get = HttpUtility.UrlDecode(matches.Groups[1].Value.ToString());
                        Console.WriteLine("Search: " + get);

                        Index.SearchResult result = Program.ind.Search(get);
                        Dictionary<string, object> dict = new Dictionary<string, object>();
                        dict.Add("found", result.foundDocs.Count);
                        dict.Add("result", result.foundDocs);


                        try
                        {
                            string output = JsonConvert.SerializeObject(dict, Formatting.Indented, new JsonSerializerSettings
                            {
                                ReferenceLoopHandling = ReferenceLoopHandling.Ignore
                            });
                            Resp += output;
                        }
                        catch (Exception e)
                        {
                            Resp = GetError(502) + "\n" + e.Message;
                        }

                    }
                    catch (Exception e)
                    {

                    }
                }
                else
                    Resp += "<pre>DOCODO Search Engine\nCopyright (c) 2018 Alexey Zakharchenko</pre>";

                Buffer = Encoding.UTF8.GetBytes(Resp);
                client.GetStream().Write(Buffer);

                client.Close();
            }
        }

    }
}