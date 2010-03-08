using System;
using System.Windows.Forms;
using ExportSqlCE;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;

namespace SqlCeScripter
{
    internal class DatabaseMenuItem : ToolsMenuItemBase, IWinformsMenuHandler
    {
        internal DatabaseMenuItem()
        {
            this.Text = "Script database...";
        }

        protected override void Invoke()
        {
            
        }
        
        public override object Clone()
        {
            return new DatabaseMenuItem();
        }

        #region IWinformsMenuHandler Members

        public System.Windows.Forms.ToolStripItem[] GetMenuItems()
        {
            
            ToolStripMenuItem item = new ToolStripMenuItem("Script Database");

            ToolStripMenuItem dbItem = new ToolStripMenuItem("Schema and Data...");
            dbItem.Tag = Scope.SchemaData;
            dbItem.Click += new EventHandler(item_Click);

            ToolStripMenuItem dbItem1 = new ToolStripMenuItem("Schema and Data with BLOB files...");
            dbItem1.Tag = Scope.SchemaDataBlobs;
            dbItem1.Click += new EventHandler(item_Click);

            ToolStripMenuItem dbItem2 = new ToolStripMenuItem("Schema...");
            dbItem2.Tag = Scope.Schema;
            dbItem2.Click += new EventHandler(item_Click);

            item.DropDownItems.Add(dbItem);
            item.DropDownItems.Add(dbItem1);
            item.DropDownItems.Add(dbItem2);

            item.DropDownItems.Add(new ToolStripSeparator());

            ToolStripMenuItem aboutItem = new ToolStripMenuItem("About...");
            aboutItem.Image = Properties.Resources.data_out;
            aboutItem.Click += new EventHandler(AboutItem_Click);
            item.DropDownItems.Add(aboutItem);

            return new ToolStripItem[] { item };

        }

        #endregion

        void AboutItem_Click(object sender, EventArgs e)
        {
            new AboutDlg().ShowDialog();
        }

        void item_Click(object sender, EventArgs e)
        {
            Connect.Monitor.TrackFeature("Database.Script");

            string connectionString = Helper.FixConnectionString(this.Parent.Connection.ConnectionString, this.Parent.Connection.ConnectionTimeout);
            string fileName;
            
            ToolStripMenuItem item = (ToolStripMenuItem)sender;
            Scope scope = (Scope)item.Tag;

            SaveFileDialog fd = new SaveFileDialog();
            fd.AutoUpgradeEnabled = true;
            fd.Title = "Save generated database script as";
            fd.Filter = "SQL Server Compact Script (*.sqlce)|*.sqlce|SQL Server Script (*.sql)|*.sql|All Files(*.*)|";
            fd.OverwritePrompt = true;
            fd.ValidateNames = true;
            if (fd.ShowDialog() == DialogResult.OK)
            {
                fileName = fd.FileName;
                try
                {
                    using (IRepository repository = new DBRepository(connectionString))
                    {
                        System.Windows.Forms.MessageBox.Show(Helper.ScriptDatabaseToFile(fileName, scope, repository));
                    }
                }
                catch (System.Data.SqlServerCe.SqlCeException sqlCe)
                {
                    Connect.Monitor.TrackException((Exception)sqlCe);
                    Connect.ShowErrors(sqlCe);
                }
                catch (Exception ex)
                {
                    Connect.Monitor.TrackException(ex);
                    MessageBox.Show(ex.ToString());
                }
            }

        }

    }
}
