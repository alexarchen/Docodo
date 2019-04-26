using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Docodo
{
    /* Data Source to index text & blob fields in database */

    public abstract class DBDataSourceBase: QueuedDataSource<IIndexDocument>
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
        // File - relative to the base path file name stored in field <FieldName>,
        // Blob - documents are stored in blob (first or <FieldName>)
        // Text - index texts stored in TEXT fields (or one field <FieldName>) 

        public DBDataSourceBase(string name, string basepath, string connect, string select, IndexType indextype, string datafieldname=null) :base (name,basepath)
        {
            ConnectString = connect;
            SelectString = select;
            indexType = indextype;
            FieldName = datafieldname??"";
        }

        /// Add document from or TEXT field
        public virtual void AddRecord(string name,char[] buff, string fields, ConcurrentQueue<IIndexDocument> queue)
        {
            if ((fields != null) && (!fields.Contains("Source=")))
                fields += $"Source={Name}\n";
            Enqueue(queue,new IndexPagedTextFile(name,new string (buff),fields));
        }

        /// Add document from BLOB 
        public virtual void AddRecord(string name, Stream stream, string fields,ConcurrentQueue<IIndexDocument> queue)
        {
            bool isText = false;
            IIndexDocument doc = null;
            if ((fields != null) && (!fields.Contains("Source=")))
              fields += $"Source={Name}\n";

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
                    
                    
                        IndexPagedTextFile file = WebDataSource.FromHtml(stream, name,Name);
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
                Enqueue(queue,doc);
        }
        /// Add document stored in file fname
        public virtual void AddRecord(string name,string fname,string fields,ConcurrentQueue<IIndexDocument> queue)
        {
            if (indexType != IndexType.File) throw new InvalidDataException("Adding record of wrong IndexType");

            if ((fields != null) && (!fields.Contains("Source=")))
                fields += $"Source={Name}\n";

            IndexTextFilesDataSource.IndexedTextFile doc;
            if (fname.ToLower().EndsWith(".pdf"))
                doc = new DocumentsDataSource.IndexPDFDocument(System.IO.Path.Combine(Path,fname), this);
            else
                doc = new IndexTextFilesDataSource.IndexedTextFile(System.IO.Path.Combine(Path, fname), this);

            doc.Name = name;

            if (fields != null)
            {
              doc.headers = ()=>{ return fields; };
            }

           Enqueue(queue,doc);
        }

        protected override IIndexDocument DocumentFromItem(IIndexDocument item)
        {
            return item;
        }


    }


    public class EntityDataSource<T> : DBDataSourceBase where T : class
    {
        
        private Func<IEnumerable<T>> set;
        private Func<T, object> selectKey;
        private Func<T, string> filenameFunc;

        public EntityDataSource(string name,string basepath, Func<IEnumerable<T>> entities,IndexType indextype, string datafieldname=null,string key=null) : base(name, basepath, "", "", indextype,datafieldname)
        {
            if (indexType == IndexType.Blob) throw new Exception("Not supported");
            set = entities;
            selectKey = (i)=>i.GetType().GetProperty(key).GetValue(i);
            filenameFunc = (i) => i.GetType().GetProperty(datafieldname).GetValue(i).ToString();
        }
        public EntityDataSource(string name, Func<IEnumerable<T>> entities, Func<T,string> filenameFunc, Func<T,object> selectKey) : base(name, "", "", "", IndexType.File, "")
        {
            set = entities;
            this.selectKey = selectKey;
            this.filenameFunc = filenameFunc;
        }


        protected override void Navigate(ConcurrentQueue<IIndexDocument> queue, CancellationToken token)
        {
            List<(string name,Type type)> fields = 
             typeof(T).GetProperties().Where(p => ((p.GetMethod != null) && (p.PropertyType.IsPublic) && (!p.PropertyType.IsArray) /*&& (!p.PropertyType.IsClass)*/)).Select(p => (p.Name, p.PropertyType)).ToList() ;

            int id = 1;
            foreach (var item in set())
            {
          
                token.ThrowIfCancellationRequested();

                StringBuilder builder = new StringBuilder();
                //string primaryname = "";
                string name = ""+(id++);
                string fname = "";
                String text = "";

                name = selectKey(item).ToString();
                fname = filenameFunc(item);

                foreach (var field in fields)
                {
                    string val = item.GetType().GetProperty(field.name).GetGetMethod().Invoke(item, null)?.ToString();

                    if (val!=null)
                    {
                        //if (field.name.Equals(FieldName)) fname = val;
                        //if (field.name.ToLower().Equals("id")) primaryname = val;
                        builder.Append(field.name + "=" + val + "\n");
                    }
                }
//                if (primaryname.Length > 0) name = primaryname;

                builder.Append("Name=" + name + "\n");

                switch (indexType)
                {
                    case IndexType.File:
                        if (fname.Length > 0)
                            AddRecord(name, fname, builder.ToString(), queue);
                        break;
                    case IndexType.Text:
                        if (text.Length > 0)
                            AddRecord(name, text.ToCharArray(), builder.ToString(), queue);
                        break;

                }
            };

        }
    }

    /*
    public class MySqlDBDocSource: DBDataSourceBase
    {

        public MySqlDBDocSource(string name, string basepath, string connect, string select, IndexType indextype,string datafieldname=null) : base(name, basepath, connect, select, indextype,datafieldname)
        {

        }
        protected override void Navigate(ConcurrentQueue<IIndexDocument> queue, CancellationToken token)
        {

            MySqlConnection conn = new MySqlConnection(ConnectString);
            conn.Open();
            MySqlCommand command = new MySqlCommand(SelectString, conn);
            MySqlDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                token.ThrowIfCancellationRequested();

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
                builder.Append("Name=" + name+"\n");

                switch (indexType) {
                    case IndexType.File:
                    if (fname.Length > 0)
                       AddRecord(name, fname, builder.ToString(),queue);
                        break;
                    case IndexType.Blob:
                        if (datastream != null)
                        {
                            AddRecord(name, datastream, builder.ToString(),queue);
                        }
                        break;
                    case IndexType.Text:
                        if (text.Length > 0)
                            AddRecord(name, text.ToCharArray(), builder.ToString(),queue);
                        break;
                
                }
            }

            reader.Close();
            conn.Close();
        }
    }
    */
}
