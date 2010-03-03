using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Data.SqlServerCe;
using System.IO;

namespace SqlCeScripter
{
    public partial class ResultsetGrid : UserControl, IDisposable 
    {
        private SqlCeConnection _conn = null;
        private SqlCeResultSet resultSet = null;
        private DataGridViewSearch dgs = null;
        private ContextMenuStrip imageContext = new ContextMenuStrip();
        private DataGridViewCell selectedCell = null;

        public ResultsetGrid()
        {
            InitializeComponent();
        }

        private void ResultsetGrid_Load(object sender, EventArgs e)
        {
            try
            {
                _conn = new SqlCeConnection(Connect.ConnectionString);
                this.dataGridView1.AutoGenerateColumns = true;
                this.dataGridView1.DataError += new DataGridViewDataErrorEventHandler(dataGridView1_DataError);
                SqlCeCommand cmd = new SqlCeCommand();
                cmd.Connection = _conn;
                _conn.Open();
                if (Connect.ViewsSelected)
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = string.Format("SELECT * FROM {0}", Connect.CurrentTable);
                    // Must use dataset to disable EnforceConstraints
                    DataSet dataSet = new DataSet();
                    dataSet.EnforceConstraints = false;
                    string[] tables = new string[1];
                    tables[0] = "table1";
                    SqlCeDataReader rdr = cmd.ExecuteReader();
                    dataSet.Load(rdr, LoadOption.OverwriteChanges, tables);
                    dataSet.Tables[0].DefaultView.AllowDelete = false;
                    dataSet.Tables[0].DefaultView.AllowEdit = false;
                    dataSet.Tables[0].DefaultView.AllowNew = false;
                    this.bindingSource1.DataSource = dataSet.Tables[0];
                    this.dataGridView1.ReadOnly = true;
                }
                else
                {
                    cmd.CommandType = CommandType.TableDirect;
                    cmd.CommandText = Connect.CurrentTable;
                    resultSet = cmd.ExecuteResultSet(ResultSetOptions.Scrollable | ResultSetOptions.Updatable);
                    this.bindingSource1.DataSource = resultSet;
                    imageContext.Items.Add("Import Image", null, new EventHandler(ImportImage));
                    imageContext.Items.Add("Export Image", null, new EventHandler(ExportImage));
                    imageContext.Items.Add("Delete Image", null, new EventHandler(DeleteImage));
                }
                this.dataGridView1.ClipboardCopyMode = DataGridViewClipboardCopyMode.EnableAlwaysIncludeHeaderText;
                this.dataGridView1.AllowUserToOrderColumns = true;
                this.dataGridView1.AutoResizeColumns();
                this.dataGridView1.TopLeftHeaderCell.Value = "About";
                // Search - how to make it work with SqlCeResultSet
                this.dataGridView1.KeyDown += new KeyEventHandler(dataGridView1_KeyDown);
                dgs = new DataGridViewSearch(this.dataGridView1);

                // About
                dataGridView1.MouseClick += new MouseEventHandler(dataGridView1_MouseClick);

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

        void dataGridView1_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.F3)
            {
                this.dgs.ShowSearch();
            }
        }

        void dataGridView1_MouseClick(object sender, MouseEventArgs e)
        {
            DataGridView.HitTestInfo info = this.dataGridView1.HitTest(e.X, e.Y);
            if (info.ColumnIndex == -1 && info.RowIndex == -1)
            {
                using (AboutDlg about = new AboutDlg())
                {
                    about.ShowDialog();
                }
            }
        }

        void dataGridView1_DataError(object sender, DataGridViewDataErrorEventArgs e)
        {
            MessageBox.Show(string.Format("DataGridView error: {0}, row: {1}, column: {2}", e.Exception.Message, e.RowIndex + 1, e.ColumnIndex + 1)); 
        }

        private void dataGridView1_MouseUp(object sender, MouseEventArgs e)
        {
            selectedCell = null;
            // Load context menu on right mouse click
            if (e.Button == MouseButtons.Right)
            {
                DataGridView.HitTestInfo hitTestInfo;
                hitTestInfo = dataGridView1.HitTest(e.X, e.Y);
                if (hitTestInfo.Type == DataGridViewHitTestType.Cell)
                {
                    DataGridViewCell cell = dataGridView1[hitTestInfo.ColumnIndex, hitTestInfo.RowIndex];
                    if (cell.FormattedValueType == typeof(System.Drawing.Image))
                    {
                        selectedCell = cell;
                        imageContext.Show(dataGridView1, new Point(e.X, e.Y));
                    }
                }
            }
        }

        void ImportImage(object sender, EventArgs e)
        {
            if (selectedCell != null)
            {
                using (OpenFileDialog fd = new OpenFileDialog())
                {
                    fd.Multiselect = false;
                    if (fd.ShowDialog() == DialogResult.OK)
                    {
                        selectedCell.Value = File.ReadAllBytes(fd.FileName);
                    }
                }
            }
        }

        void ExportImage(object sender, EventArgs e)
        {
            if (selectedCell != null && selectedCell.Value != null)
            {
                using (SaveFileDialog fd = new SaveFileDialog())
                {
                    if (fd.ShowDialog() == DialogResult.OK)
                    {
                        File.WriteAllBytes(fd.FileName, (byte[])selectedCell.Value);
                    }
                }
            }
        }

        void DeleteImage(object sender, EventArgs e)
        {
            if (selectedCell != null)
            {
                selectedCell.Value = null;
            }
        }

        #region IDisposable Members

        void IDisposable.Dispose()
        {
            if (resultSet != null)
            {
                resultSet.Dispose();
            }
            if (_conn != null)
            {
                _conn.Dispose();
            }
        }

        #endregion

       
    }
}
