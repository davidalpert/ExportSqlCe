using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using System.Diagnostics;
using System.Reflection;

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
