using System;
using System.Windows.Forms;
using ExportSqlCE;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;

namespace SqlCeScripter
{
    public class TableMenuItem : ToolsMenuItemBase, IWinformsMenuHandler
    {
        public TableMenuItem()
        {
            this.Text = "Script";
        }

        protected override void Invoke()
        {
            
        }
        
        public override object Clone()
        {
            return new TableMenuItem();
        }

        #region IWinformsMenuHandler Members

        public System.Windows.Forms.ToolStripItem[] GetMenuItems()
        {
            ToolStripMenuItem item = new ToolStripMenuItem("Script");

            ToolStripMenuItem insertItem = new ToolStripMenuItem("Table as CREATE");
            insertItem.Tag = false;
            insertItem.Click += new EventHandler(item_Click);

            ToolStripMenuItem insertItem2 = new ToolStripMenuItem("Data as INSERT");
            insertItem2.Tag = true;
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
            ToolStripMenuItem item = (ToolStripMenuItem)sender;
            bool scriptData = (bool)item.Tag;
            try
            {
                string connectionString = this.Parent.Connection.ConnectionString;
                connectionString = connectionString.Replace(";Timeout = \"30\"", string.Empty);
                
                using (IRepository repository = new DBRepository(connectionString))
                {
                    var generator = new Generator(repository, string.Empty);
                    // create new document
                    ServiceCache.ScriptFactory.CreateNewBlankScript(Microsoft.SqlServer.Management.UI.VSIntegration.Editors.ScriptType.SqlCe);                    

                    // Generate script
                    if (scriptData)
                    {
                        generator.GenerateTableData(this.Parent.Name);
                    }
                    else
                    {
                        generator.GenerateTableScript(this.Parent.Name);
                    }

                    // insert SQL script to document
                    EnvDTE.TextDocument doc = (EnvDTE.TextDocument)ServiceCache.ExtensibilityModel.Application.ActiveDocument.Object(null);

                    doc.EndPoint.CreateEditPoint().Insert(generator.GeneratedScript);
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
