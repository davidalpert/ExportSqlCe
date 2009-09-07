using System;
using System.Text;
using EnvDTE;
using Extensibility;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;
using Microsoft.SqlServer.Management.SqlStudio.Explorer;

namespace SqlCeScripter
{
    /// <summary>The object for implementing an Add-in.</summary>
    /// <seealso class='IDTExtensibility2' />
    public class Connect : IDTExtensibility2
    {
        private HierarchyObject _serverMenu = null;
        private HierarchyObject _tableMenu = null;
       
        string urnPath = "SqlServerCe";
        string tableUrnPath = "SqlServerCe/UsrTable";

        public Connect()
        {
        }

        public void OnConnection(object application, ext_ConnectMode connectMode, object addInInst, ref Array custom)
        {
            
#if R2
            ObjectExplorerService objExplorerService = (ObjectExplorerService)ServiceCache.ServiceProvider.GetService(typeof(IObjectExplorerService));
            
            INodeInformation[] nodes;
            int nodeCount;
            objExplorerService.GetSelectedNodes(out nodeCount, out nodes);

            ContextService cs = null;
            for (int i = 0; i < objExplorerService.Container.Components.Count; i++)
            {
                if (objExplorerService.Container.Components[i].GetType() == typeof(ContextService))
                {
                    cs = (ContextService)objExplorerService.Container.Components[i];
                }
            }            
            cs.ObjectExplorerContext.CurrentContextChanged += new NodesChangedEventHandler(Provider_SelectionChanged); 
#else
            IObjectExplorerService objectExplorer = (IObjectExplorerService)ServiceCache.ServiceProvider.GetService(typeof(IObjectExplorerService));
            IObjectExplorerEventProvider provider = (IObjectExplorerEventProvider)objectExplorer.GetService(typeof(IObjectExplorerEventProvider));
            provider.SelectionChanged += new NodesChangedEventHandler(Provider_SelectionChanged);
#endif
        }

        private void Provider_SelectionChanged(object sender, NodesChangedEventArgs args)
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
                System.Diagnostics.Debug.WriteLine(node.UrnPath);
                System.Diagnostics.Debug.WriteLine(node.Name);
                System.Diagnostics.Debug.WriteLine(node.Context);
            }

            if (node != null && _serverMenu == null &&
                urnPath == node.UrnPath)
            {
                _serverMenu = (HierarchyObject)node.GetService(typeof(IMenuHandler));
                
                _serverMenu.AddChild(string.Empty, new ToolStripSeparatorMenuItem());
                MenuItem item = new MenuItem();
                _serverMenu.AddChild(string.Empty, item);
            }

            if (node != null && _tableMenu == null &&
                tableUrnPath == node.UrnPath)
            {
                _tableMenu = (HierarchyObject)node.GetService(typeof(IMenuHandler));

                _tableMenu.AddChild(string.Empty, new ToolStripSeparatorMenuItem());

                ScriptTableMenuItem item = new ScriptTableMenuItem();
                _tableMenu.AddChild(string.Empty, item);
                _tableMenu.AddChild(string.Empty, new ToolStripSeparatorMenuItem());
                ImportTableMenuItem item2 = new ImportTableMenuItem();
                _tableMenu.AddChild(string.Empty, item2);
                _tableMenu.AddChild(string.Empty, new ToolStripSeparatorMenuItem());
                RenameTableMenuItem item3 = new RenameTableMenuItem();
                _tableMenu.AddChild(string.Empty, item3);
            }


        }

        public void OnDisconnection(ext_DisconnectMode disconnectMode, ref Array custom)
        {
        }

        public void OnAddInsUpdate(ref Array custom)
        {
        }

        public void OnStartupComplete(ref Array custom)
        {
        }

        public void OnBeginShutdown(ref Array custom)
        {
        }

        internal static void ShowErrors(System.Data.SqlServerCe.SqlCeException e)
        {
            System.Data.SqlServerCe.SqlCeErrorCollection errorCollection = e.Errors;

            StringBuilder bld = new StringBuilder();
            Exception inner = e.InnerException;

            if (null != inner)
            {
                Console.WriteLine("Inner Exception: " + inner.ToString());
            }
            // Enumerate the errors to a message box.
            foreach (System.Data.SqlServerCe.SqlCeError err in errorCollection)
            {
                bld.Append("\n Error Code: " + err.HResult.ToString("X", System.Globalization.CultureInfo.InvariantCulture));
                bld.Append("\n Message   : " + err.Message);
                bld.Append("\n Minor Err.: " + err.NativeError);
                bld.Append("\n Source    : " + err.Source);

                // Enumerate each numeric parameter for the error.
                foreach (int numPar in err.NumericErrorParameters)
                {
                    if (0 != numPar) bld.Append("\n Num. Par. : " + numPar);
                }

                // Enumerate each string parameter for the error.
                foreach (string errPar in err.ErrorParameters)
                {
                    if (!string.IsNullOrEmpty(errPar)) bld.Append("\n Err. Par. : " + errPar);
                }

                System.Windows.Forms.MessageBox.Show(bld.ToString());
                bld.Remove(0, bld.Length);
            }
        }

    }
}