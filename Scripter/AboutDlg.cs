using System.Diagnostics;
using System.Reflection;
using System.Windows.Forms;

namespace SqlCeScripter
{
    internal partial class AboutDlg : Form
    {
        private string downloadUri;

        internal AboutDlg()
        {
            InitializeComponent();
            this.lblVersion.Text = Assembly.GetExecutingAssembly().GetName().Version.ToString();
        }

        private void linkLabel3_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)
        {
            LinkLabel label = (LinkLabel)sender;
            Process.Start(label.Text);
        }

        private void linkLabelNewVersion_LinkClicked(object sender, LinkLabelLinkClickedEventArgs e)
        {
            if (!string.IsNullOrEmpty(this.downloadUri))
            {
                Process.Start(this.downloadUri);    
            }
        }
    }
}
