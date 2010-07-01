using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlServerCe;

namespace ErikEJ.SqlCeScripting
{

    // custom attributes 
    public class SyncArgs : System.EventArgs
    {

        private string message;
        private Exception exception;

        public SyncArgs(string m, Exception ex)
        {
            this.message = m;
            this.exception = ex;
        }

        public string Message()
        {
            return message;
        }

        public Exception Exception()
        {
            return exception;
        }


    } 


    public class SqlCeReplicationHelper
    {
        private string tableName;
        private int percentage;
        private SqlCeReplication repl;

        // delegate declaration 
        public delegate void CompletedHandler(object sender, SyncArgs ca);

        // event declaration 
        public event CompletedHandler Completed;

        // delegate declaration 
        public delegate void ProgressHandler(object sender, SyncArgs ca);

        // event declaration 
        public event ProgressHandler Progress; 

        public SqlCeReplicationHelper(string connectionString, string url, string publisher, string publicationDatabase, string publication, string subscriber, string hostName, bool useNT, string internetUsername, string internetPassword, string publisherUsername, string publisherPassword, bool isNew)
        {
            this.repl = new SqlCeReplication();
            repl.SubscriberConnectionString = connectionString;

            if (isNew)
            {
                repl.AddSubscription(AddOption.ExistingDatabase);
            }
            if (useNT)
            {
                repl.PublisherSecurityMode = SecurityType.NTAuthentication;
            }
            else
            {
                repl.PublisherSecurityMode = SecurityType.DBAuthentication;
            }

            repl.Publisher = publisher;
            repl.PublisherLogin = publisherUsername;
            repl.PublisherPassword = publisherPassword;
            repl.PublisherDatabase = publicationDatabase;
            repl.Publication = publication;
            repl.InternetUrl = url;
            repl.InternetLogin = internetUsername;
            repl.InternetPassword = internetPassword;
            repl.Subscriber = subscriber;
            repl.HostName = hostName;
            
        }

        public static void DropPublication(string connectionString, string publicationLabel)
        {
            string[] vals = publicationLabel.Split(':');
            string publisher = vals[0];
            string publicationDatabase = vals[1];
            string publication = vals[2];
            using (SqlCeReplication repl = new SqlCeReplication())
            {
                repl.SubscriberConnectionString = connectionString;
                repl.Publisher = publisher;
                repl.Publication = publication;
                repl.PublisherDatabase = publicationDatabase;
                repl.LoadProperties();
                repl.DropSubscription(DropOption.LeaveDatabase);
            }
        }

        public static ReplicationProperties GetProperties(string connectionString, string publicationLabel)
        {
            string[] vals = publicationLabel.Split(':');
            string publisher = vals[0];
            string publicationDatabase = vals[1];
            string publication = vals[2];
            using (SqlCeReplication repl = new SqlCeReplication())
            {
                repl.SubscriberConnectionString = connectionString;
                repl.Publisher = publisher;
                repl.Publication = publication;
                repl.PublisherDatabase = publicationDatabase;
                repl.LoadProperties();
                var props = new ReplicationProperties();
                props.InternetLogin = repl.InternetLogin;
                props.InternetPassword = repl.InternetPassword;
                props.InternetUrl = repl.InternetUrl;
                props.Publication = repl.Publication;
                props.Publisher = repl.Publisher;
                props.PublisherDatabase = repl.PublisherDatabase;
                props.PublisherLogin = repl.PublisherLogin;
                props.PublisherPassword = repl.PublisherPassword;
                if (repl.PublisherSecurityMode == SecurityType.NTAuthentication)
                    props.UseNT = true;
                props.Subscriber = repl.Subscriber;
                props.SubscriberConnectionString = repl.SubscriberConnectionString;
                props.HostName = repl.HostName;
                return props;
            }
        }

        private void SyncCompletedCallback(IAsyncResult ar)
        {
            try
            {
                SqlCeReplication repl = (SqlCeReplication)ar.AsyncState;

                repl.EndSynchronize(ar);
                repl.SaveProperties();

                SyncArgs args = new SyncArgs("Successfully completed sync", null);
                Completed(this, args);
            
            }
            catch (SqlCeException e)
            {
                SyncArgs args = new SyncArgs("Errors", e);
                Completed(this, args);
            }
        }

        private void OnStartTableUploadCallback(IAsyncResult ar, string tableName)
        {
            this.tableName = tableName;
            var args = new SyncArgs("Began uploading table : " + tableName, null);
            Progress(this, args);
        }

        private void OnSynchronizationCallback(IAsyncResult ar, int percentComplete)
        {
            this.percentage = percentComplete;
            var args = new SyncArgs("Sync with SQL Server is " + percentage.ToString() + "% complete.", null);
            Progress(this, args);

        }

        private void OnStartTableDownloadCallback(IAsyncResult ar, string tableName)
        {
            this.tableName = tableName;
            var args = new SyncArgs("Began downloading table : " + tableName, null);
            Progress(this, args);
        }

        public void Synchronize()
        {
            if (this.repl != null)
            {
                IAsyncResult ar = repl.BeginSynchronize(
                    new AsyncCallback(this.SyncCompletedCallback),
                    new OnStartTableUpload(this.OnStartTableUploadCallback),
                    new OnStartTableDownload(this.OnStartTableDownloadCallback),
                    new OnSynchronization(this.OnSynchronizationCallback),
                    repl);
            }
        }

    }

    public struct ReplicationProperties
    {
        public string SubscriberConnectionString { get; set; }
        public bool UseNT { get; set; }
        public string Publisher { get; set; }
        public string PublisherLogin { get; set; }
        public string PublisherPassword { get; set; }
        public string PublisherDatabase { get; set; }
        public string Publication { get; set; }
        public string InternetUrl { get; set; }            
        public string InternetLogin { get; set; }
        public string InternetPassword { get; set; }
        public string Subscriber { get; set; }
        public string HostName { get; set; }

    }
}

