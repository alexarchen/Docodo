using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Docodo
{
    /* Data Source to index text & blob fields in database */

    public abstract class DBDataSourceBase: IndexTextFilesDataSource
    {

        public string ConnectString; // connection string to DB
        public string SelectString;  // select SQL query for table or view
        public enum IndexType { File, Blob, Text };   // File, Blob or Text
        public IndexType indexType;
        public string FieldName;     // storage for :<FieldName>

        /* name - unique name of the datasource,
         * basepath - base part of the documents path, in the database there are relative paths 
         * connect - connect string,
         * select - select SQL
         * indextype: */
        // File:<FieldName> - relative to the base path file name stored in field <FieldName>,
        // Blob[:<FieldName>] - documents are stored in blob (first or <FieldName>)
        // Text[:<FieldName>] - index texts stored in TEXT fields (or one field <FieldName>) 

        public DBDataSourceBase(string name, string basepath, string connect, string select, string indextype) :base (name,basepath)
        {
            ConnectString = connect;
            SelectString = select;
            switch (indextype.ToLower().Split(':')[0])
            {
                case "Text":
                indexType = IndexType.Text;
                break;
                case "File":
                    indexType = IndexType.File;
                    break;
                case "Blob":
                    indexType = IndexType.Blob;
                    break;
            }
            FieldName = "";
            if (indextype.Contains(':'))
                FieldName = indextype.Split(':')[1];
        }

        private ConcurrentQueue<IIndexDocument> records = new ConcurrentQueue<IIndexDocument>();
        bool NavDone = false;
        bool Navigating = false;
        CancellationTokenSource tokenSource { get; } = new CancellationTokenSource();
        public CancellationToken cancellationToken { get => tokenSource.Token; }
        // add to records Queue, called when need to enumerate all records in database or table
        // use AddDocRecord to add record to queue, 
        // check cancellationToken
        public abstract void AddRecords();

        // Add document from or TEXT field
        public virtual void AddRecord(string name,char[] buff, string fields)
        {
            records.Enqueue(new IndexPagedTextFile(name,new string (buff),fields));
        }

        // Add document from BLOB 
        public virtual void AddRecord(string name, Stream stream, string fields)
        {
            bool isText = false;
            IIndexDocument doc = null;

            if ((indexType==IndexType.File) || (indexType != IndexType.Blob)) throw new InvalidDataException("Adding record of wrong IndexType");

                BinaryReader reader = new BinaryReader(stream);
                byte[] buff = new byte[4000];
                reader.Read(buff, 0, 4000);
                String det = Encoding.UTF8.GetString(buff, 0, buff.Length);
                stream.Seek(0, SeekOrigin.Begin);
                reader.Dispose();

                // detect type
                if ((buff[0] == '%') && (buff[1] == 'P') && (buff[2] == 'D') && (buff[3] == 'F'))
                {
                    DocumentsDataSource.IndexPDFDocument pdf = new DocumentsDataSource.IndexPDFDocument(name, stream, this);
                    if (fields != null)
                        pdf.headers = () => { return (fields); };
                    doc = pdf;
                }
                else
                if (det.Contains("<html"))
                {
                    
                    
                        IndexPagedTextFile file = WebDataSource.FromHtml(stream, name);
                        if (fields != null)
                            file.SetHeaders(fields);
                    
                }
                else
                {
                    // detect charset
                    Ude.CharsetDetector detector = new Ude.CharsetDetector();
                    detector.Feed(buff, 0, buff.Length);
                    detector.DataEnd();
                    if (detector.Charset != null)
                    {
                      Encoding enc = Portable.Text.Encoding.GetEncoding(detector.Charset);
                       using (StreamReader sreader = new StreamReader(stream, enc, false)) {
                        doc = new IndexPagedTextFile("", sreader.ReadToEnd(), fields != null ? fields : "");
                       }
                    }

                }

          if (doc!=null)
                records.Enqueue(doc);
        }
        // Add document stored in file fname
        public virtual void AddRecord(string name,string fname,string fields)
        {
            if (indexType != IndexType.File) throw new InvalidDataException("Adding record of wrong IndexType");

                IndexTextFilesDataSource.IndexedTextFile doc;
            if (fname.ToLower().EndsWith(".pdf"))
                doc = new DocumentsDataSource.IndexPDFDocument(path + "\\" + fname, this);
            else
                doc = new IndexedTextFile(path+"\\"+fname, this);

            doc.Name = name;

            if (fields != null)
            {
              doc.headers = ()=>{ return fields; };
            }

            records.Enqueue(doc);
        }

        public override IIndexDocument Next(bool bwait)
        {

            IIndexDocument file;
            while ((!records.TryDequeue(out file)) && (bwait) && (Navigating))
            {
                Thread.Sleep(100);
            }

            return file;
        }

        public override void Reset()
        {
           if (!Navigating)
            {
                Task.Factory.StartNew(() =>
                {
                    Navigating = true;
                    AddRecords();
                    Navigating = false;
                });

            }

        }

        public override void Dispose()
        {
            tokenSource.Dispose();
        }
        
    }

    /* Data Source to index documents kept in blob fields 
    public class DBBlobDataSource: IIndexDataSource
    {

    }
    */


    public class MySqlDBDocSource: DBDataSourceBase
    {

        public MySqlDBDocSource(string name, string basepath, string connect, string select,string indextype) : base(name, basepath,connect,select,indextype)
        {
        }
        public override void AddRecords()
        {

            MySqlConnection conn = new MySqlConnection(ConnectString);
            conn.Open();
            MySqlCommand command = new MySqlCommand(SelectString, conn);
            MySqlDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                StringBuilder builder = new StringBuilder();
                string primaryname = "";
                string name = "";
                string fname = "";
                Stream datastream = null;
                String text = "";

                DataTable table = reader.GetSchemaTable();
                for (int q = 0; q < reader.FieldCount; q++)
                {

                    MySqlDbType type = (MySqlDbType)(int)table.Rows[q]["ProviderType"];
                    if ((bool)table.Rows[q]["IsUnique"])
                        if (name.Length == 0) name = reader.GetString(q);
                    if ((bool)table.Rows[q]["IsKey"])
                        if (primaryname.Length == 0) primaryname = reader.GetString(q);
                    if (indexType == IndexType.File)
                        if (reader.GetName(q).Equals(FieldName))
                        {
                            fname = reader.GetString(q);
                        }

                    if (type == MySqlDbType.Blob)
                    {
                        if (indexType == IndexType.Blob)
                        {
                            datastream = reader.GetStream(q);
                        }
                        continue;
                    }

                    if (type == MySqlDbType.Text)
                    {
                        if (indexType == IndexType.Text)
                        {
                            text = reader.GetString(q);
                        }
                        continue;
                    }
                    if (!reader.IsDBNull(q))
                        builder.Append(reader.GetName(q) + "=" + reader.GetString(q) + "\n");

                }

                if (primaryname.Length > 0) name = primaryname;
                switch (indexType) {
                    case IndexType.File:
                    if (fname.Length > 0)
                       AddRecord(name, fname, builder.ToString());
                        break;
                    case IndexType.Blob:
                        if (datastream != null)
                        {
                            AddRecord(name, datastream, builder.ToString());
                        }
                        break;
                    case IndexType.Text:
                        if (text.Length > 0)
                            AddRecord(name, text.ToCharArray(), builder.ToString());
                        break;
                
                }
            }

            reader.Close();
            conn.Close();
        }
    }

}
