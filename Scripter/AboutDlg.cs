using System.Diagnostics;
using System.Reflection;
using System.Windows.Forms;

namespace SqlCeScripter
{
    internal partial class AboutDlg : Form
    {
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
    }
}
