using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Forms;

namespace SqlCeScripter
{
    // Thanks to http://www.codeproject.com/KB/grid/ExtendedDataGridView.aspx
    internal class DataGridViewSearch
    {
        PanelQuickSearch m_pnlQuickSearch;

        DataGridView dgv;

        internal DataGridViewSearch(DataGridView dgv)
        {
            this.dgv = dgv;
        }

        internal void ShowSearch()
        {

            if (dgv.SortedColumn != null)            
            {
                if (m_pnlQuickSearch == null)
                {
                    m_pnlQuickSearch = new PanelQuickSearch();
                    dgv.Controls.Add(m_pnlQuickSearch);
                    m_pnlQuickSearch.SearchChanged += m_pnlQuickSearch_SearchChanged;
                }

                if (dgv.SelectedRows.Count > 0)
                    m_pnlQuickSearch.Search = dgv.SelectedRows[0].Cells[dgv.SortedColumn.Index].Value.ToString();
                m_pnlQuickSearch.Column = dgv.SortedColumn.HeaderText;
                m_pnlQuickSearch.Show();
                m_pnlQuickSearch.Focus();
            }
        }

        void m_pnlQuickSearch_SearchChanged(string search)
        {
            foreach (DataGridViewRow row in dgv.SelectedRows)
                row.Selected = false;

            if (dgv.SortOrder == SortOrder.Ascending)
                dgv.Rows[BinarySearchAsc(search)].Selected = true;
            else
                dgv.Rows[BinarySearchDesc(search)].Selected = true;

            dgv.FirstDisplayedScrollingRowIndex = dgv.SelectedRows[0].Index;
        }

        int BinarySearchAsc(string value)
        {
            int max     = dgv.Rows.Count - 1,
                min     = 0,
                current,
                compare;

            int sortedColumn = dgv.SortedColumn.Index;

            while (max >= min)
            {
                current = (max - min) / 2 + min;

                compare = dgv[sortedColumn, current].Value.ToString().CompareTo(value);

                if (compare > 0)
                    max = current - 1;
                else if (compare < 0)
                    min = current + 1;
                else
                    return current;
            }

            if (min >= dgv.Rows.Count)
                return dgv.Rows.Count - 1;

            return min;
        }

        int BinarySearchDesc(string value)
        {
            int max     = dgv.Rows.Count - 1,
                min     = 0,
                current,
                compare;

            int sortedColumn = dgv.SelectedColumns[0].Index;

            while (max >= min)
            {
                current = (max - min) / 2 + min;

                compare = dgv[sortedColumn, current].Value.ToString().CompareTo(value);

                if (compare < 0)
                    max = current - 1;
                else if (compare > 0)
                    min = current + 1;
                else
                    return current;
            }

            if (max < 0)
                return 0;

            return max;
        }
    }
}
