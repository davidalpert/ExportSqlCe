using System;
using System.Windows.Forms;
using ExportSqlCE;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;

namespace SqlCeScripter
{
    internal class MenuItem : ToolsMenuItemBase, IWinformsMenuHandler
    {
        internal MenuItem()
        {
            this.Text = "Script database...";
        }

        protected override void Invoke()
        {
            
        }
        
        public override object Clone()
        {
            return new MenuItem();
        }

        #region IWinformsMenuHandler Members

        public System.Windows.Forms.ToolStripItem[] GetMenuItems()
        {
            ToolStripMenuItem item = new ToolStripMenuItem("Script Database");

            ToolStripMenuItem insertItem = new ToolStripMenuItem("Schema and Data...");
            insertItem.Tag = true;
            insertItem.Click += new EventHandler(item_Click);

            ToolStripMenuItem insertItem2 = new ToolStripMenuItem("Schema...");
            insertItem2.Tag = false;
            insertItem2.Click += new EventHandler(item_Click);

            item.DropDownItems.Add(insertItem);
            item.DropDownItems.Add(insertItem2);

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
            string connectionString = this.Parent.Connection.ConnectionString;
            string fileName;
            
            ToolStripMenuItem item = (ToolStripMenuItem)sender;
            bool scriptData = (bool)item.Tag;

            connectionString = connectionString.Replace(";Timeout = \"30\"", string.Empty);
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
                        var generator = new Generator(repository, fileName);
                        generator.GenerateAllAndSave(scriptData);
                        MessageBox.Show(string.Format("{0} successfully generated", fileName), "ExportSqlCe");
                    }
                }
                catch (System.Data.SqlServerCe.SqlCeException sqlCe)
                {
                    Connect.ShowErrors(sqlCe);
                }
                catch (Exception ex)
                {
                    MessageBox.Show(ex.ToString());
                }
            }

        }
    }
}
