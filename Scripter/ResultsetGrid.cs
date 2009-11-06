using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Data;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Data.SqlServerCe;

namespace SqlCeScripter.Scripter
{
    public partial class ResultsetGrid : UserControl
    {
        private SqlCeConnection _conn;

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
                    SqlCeResultSet resultSet = cmd.ExecuteResultSet(ResultSetOptions.Scrollable | ResultSetOptions.Updatable);
                    this.bindingSource1.DataSource = resultSet;
                }
                this.dataGridView1.ClipboardCopyMode = DataGridViewClipboardCopyMode.EnableAlwaysIncludeHeaderText;
                this.dataGridView1.AllowUserToOrderColumns = true;
                this.dataGridView1.AutoResizeColumns();
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

        void dataGridView1_DataError(object sender, DataGridViewDataErrorEventArgs e)
        {
            MessageBox.Show(string.Format("DataGridView error: {0}, row: {1}, column: {2}", e.Exception.Message, e.RowIndex + 1, e.ColumnIndex + 1)); 
        }

       
    }
}
