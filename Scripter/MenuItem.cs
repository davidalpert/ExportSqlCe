using System;
using System.Windows.Forms;
using ExportSqlCE;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;

namespace SqlCeScripter
{
    public class MenuItem : ToolsMenuItemBase, IWinformsMenuHandler
    {
        public MenuItem()
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
            ToolStripMenuItem item = new ToolStripMenuItem("Script Database...");
            item.Click += new EventHandler(item_Click);
            item.Image = SqlCeScripter.Properties.Resources.data_out;
            return new ToolStripItem[] { item };
        }

        #endregion

        void item_Click(object sender, EventArgs e)
        {
            string connectionString = this.Parent.Connection.ConnectionString;
            string fileName;
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
                        generator.GenerateAllAndSave();
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
