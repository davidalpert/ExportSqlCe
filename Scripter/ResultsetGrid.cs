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

            _conn = new SqlCeConnection(Connect.ConnectionString);
            this.dataGridView1.AutoGenerateColumns = true;
            SqlCeCommand cmd = new SqlCeCommand();
            cmd.Connection = _conn;
            cmd.CommandType = CommandType.TableDirect;
            cmd.CommandText = Connect.CurrentTable;
            _conn.Open();
            SqlCeResultSet resultSet = cmd.ExecuteResultSet(ResultSetOptions.Scrollable | ResultSetOptions.Updatable);
            this.bindingSource1.DataSource = resultSet;
        }
       
    }
}
