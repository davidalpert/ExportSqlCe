using System;
using System.Windows.Forms;
using ExportSqlCE;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;
using Microsoft.SqlServer.Management.SqlStudio.Explorer;

namespace SqlCeScripter
{
    internal class RenameTableMenuItem : ToolsMenuItemBase, IWinformsMenuHandler
    {
        internal RenameTableMenuItem()
        {
            this.Text = "Rename";
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
            ToolStripMenuItem item = new ToolStripMenuItem("Rename");
            item.Click += new EventHandler(item_Click);
            return new ToolStripItem[] { item };
        }

        #endregion


        void item_Click(object sender, EventArgs e)
        {
            ToolStripMenuItem item = (ToolStripMenuItem)sender;
            try
            {
                string connectionString = Helper.FixConnectionString(this.Parent.Connection.ConnectionString, this.Parent.Connection.ConnectionTimeout);
                
                using (IRepository repository = new DBRepository(connectionString))
                {
                    using (RenameOptions ro = new RenameOptions(this.Parent.Name))
                    {
                        if (ro.ShowDialog() == DialogResult.OK && !string.IsNullOrEmpty(ro.NewName))
                        {
                            repository.RenameTable(this.Parent.Name, ro.NewName);
                            RefreshTree();
                        }
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

        private void RefreshTree()
        {
            INodeInformation[] nodes;
            int nodeCount;
#if R2
            ObjectExplorerService objectExplorer = (ObjectExplorerService)ServiceCache.ServiceProvider.GetService(typeof(IObjectExplorerService));
#else
            IObjectExplorerService objectExplorer = (IObjectExplorerService)ServiceCache.ServiceProvider.GetService(typeof(IObjectExplorerService));
#endif
            objectExplorer.GetSelectedNodes(out nodeCount, out nodes);
            INodeInformation node = (nodeCount > 0 ? nodes[0] : null);
            if (node != null)
            {
                // Set focus on "Tables" tree node
                objectExplorer.SynchronizeTree(node.Parent);
                // Refresh = F5
                SendKeys.Send("{F5}");
            }
        }

    }
}
