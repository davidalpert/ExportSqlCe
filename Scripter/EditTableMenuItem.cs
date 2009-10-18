using System;
using System.Windows.Forms;
using ExportSqlCE;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;
using Microsoft.SqlServer.Management.SqlStudio.Explorer;
using EnvDTE80;

namespace SqlCeScripter
{
    internal class EditTableMenuItem : ToolsMenuItemBase, IWinformsMenuHandler
    {
        internal EditTableMenuItem()
        {
            this.Text = "Edit";
        }

        protected override void Invoke()
        {
            
        }
        
        public override object Clone()
        {
            return new RenameTableMenuItem();
        }

        #region IWinformsMenuHandler Members


        public System.Windows.Forms.ToolStripItem[] GetMenuItems()
        {
            ToolStripMenuItem item = new ToolStripMenuItem("Edit Data");
            item.Click += new EventHandler(item_Click);
            return new ToolStripItem[] { item };
        }

        #endregion


        void item_Click(object sender, EventArgs e)
        {
             //public void CreateSessionExplorerWindow(DTE2 application, AddIn addinInstance)

            if (Connect.CurrentAddin == null)
                return;

            if (Connect.CurrentApplication == null)
                return;

            Connect.CurrentTable = this.Parent.Name;
            Connect.ConnectionString = Helper.FixConnectionString(this.Parent.Connection.ConnectionString, this.Parent.Connection.ConnectionTimeout);

            try
            {
                Windows2 windows2 = Connect.CurrentApplication.Windows as Windows2;

                if (windows2 != null)
                {
                    object controlObject = null;
                    System.Reflection.Assembly asm = System.Reflection.Assembly.GetExecutingAssembly();

                    EnvDTE.Window toolWindow = windows2.CreateToolWindow2(Connect.CurrentAddin,
                                                                   asm.Location,
                                                                   "SqlCeScripter.Scripter.ResultsetGrid",
                                                                   "Edit " + this.Parent.Name, "{452480E3-98F9-4e2d-9411-F0F6BDB67B6E}",
                                                                   ref controlObject);

                    if (toolWindow != null)
                    {
                        toolWindow.IsFloating = false;
                        toolWindow.Linkable = false;
                        toolWindow.Visible = true;
                    }
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
